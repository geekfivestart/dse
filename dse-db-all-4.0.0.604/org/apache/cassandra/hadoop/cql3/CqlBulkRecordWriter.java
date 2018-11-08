package org.apache.cassandra.hadoop.cql3;

import com.datastax.driver.core.SSLOptions;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.HadoopCompat;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.utils.NativeSSTableLoaderClient;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CqlBulkRecordWriter extends RecordWriter<Object, List<ByteBuffer>> implements org.apache.hadoop.mapred.RecordWriter<Object, List<ByteBuffer>> {
   public static final String OUTPUT_LOCATION = "mapreduce.output.bulkoutputformat.localdir";
   public static final String BUFFER_SIZE_IN_MB = "mapreduce.output.bulkoutputformat.buffersize";
   public static final String STREAM_THROTTLE_MBITS = "mapreduce.output.bulkoutputformat.streamthrottlembits";
   public static final String MAX_FAILED_HOSTS = "mapreduce.output.bulkoutputformat.maxfailedhosts";
   public static final String IGNORE_HOSTS = "mapreduce.output.bulkoutputformat.ignorehosts";
   private final Logger logger;
   protected final Configuration conf;
   protected final int maxFailures;
   protected final int bufferSize;
   protected Closeable writer;
   protected SSTableLoader loader;
   protected Progressable progress;
   protected TaskAttemptContext context;
   protected final Set<InetAddress> ignores;
   private String keyspace;
   private String table;
   private String schema;
   private String insertStatement;
   private File outputDir;
   private boolean deleteSrc;
   private IPartitioner partitioner;

   CqlBulkRecordWriter(TaskAttemptContext context) throws IOException {
      this(HadoopCompat.getConfiguration(context));
      this.context = context;
      this.setConfigs();
   }

   CqlBulkRecordWriter(Configuration conf, Progressable progress) throws IOException {
      this(conf);
      this.progress = progress;
      this.setConfigs();
   }

   CqlBulkRecordWriter(Configuration conf) throws IOException {
      this.logger = LoggerFactory.getLogger(CqlBulkRecordWriter.class);
      this.ignores = new HashSet();
      this.conf = conf;
      DatabaseDescriptor.setStreamThroughputOutboundMegabitsPerSec(Integer.parseInt(conf.get("mapreduce.output.bulkoutputformat.streamthrottlembits", "0")));
      this.maxFailures = Integer.parseInt(conf.get("mapreduce.output.bulkoutputformat.maxfailedhosts", "0"));
      this.bufferSize = Integer.parseInt(conf.get("mapreduce.output.bulkoutputformat.buffersize", "64"));
      this.setConfigs();
   }

   private void setConfigs() throws IOException {
      this.keyspace = ConfigHelper.getOutputKeyspace(this.conf);
      this.table = ConfigHelper.getOutputColumnFamily(this.conf);
      String aliasedCf = CqlBulkOutputFormat.getTableForAlias(this.conf, this.table);
      if(aliasedCf != null) {
         this.table = aliasedCf;
      }

      this.schema = CqlBulkOutputFormat.getTableSchema(this.conf, this.table);
      this.insertStatement = CqlBulkOutputFormat.getTableInsertStatement(this.conf, this.table);
      this.outputDir = this.getTableDirectory();
      this.deleteSrc = CqlBulkOutputFormat.getDeleteSourceOnSuccess(this.conf);

      try {
         this.partitioner = ConfigHelper.getInputPartitioner(this.conf);
      } catch (Exception var4) {
         this.partitioner = Murmur3Partitioner.instance;
      }

      try {
         Iterator var2 = CqlBulkOutputFormat.getIgnoreHosts(this.conf).iterator();

         while(var2.hasNext()) {
            String hostToIgnore = (String)var2.next();
            this.ignores.add(InetAddress.getByName(hostToIgnore));
         }

      } catch (UnknownHostException var5) {
         throw new RuntimeException("Unknown host: " + var5.getMessage());
      }
   }

   protected String getOutputLocation() throws IOException {
      String dir = this.conf.get("mapreduce.output.bulkoutputformat.localdir", System.getProperty("java.io.tmpdir"));
      if(dir == null) {
         throw new IOException("Output directory not defined, if hadoop is not setting java.io.tmpdir then define mapreduce.output.bulkoutputformat.localdir");
      } else {
         return dir;
      }
   }

   private void prepareWriter() throws IOException {
      if(this.writer == null) {
         this.writer = CQLSSTableWriter.builder().forTable(this.schema).using(this.insertStatement).withPartitioner(ConfigHelper.getOutputPartitioner(this.conf)).inDirectory(this.outputDir).withBufferSizeInMB(Integer.parseInt(this.conf.get("mapreduce.output.bulkoutputformat.buffersize", "64"))).withPartitioner(this.partitioner).build();
      }

      if(this.loader == null) {
         CqlBulkRecordWriter.ExternalClient externalClient = new CqlBulkRecordWriter.ExternalClient(this.conf);
         externalClient.setTableMetadata(TableMetadataRef.forOfflineTools(CreateTableStatement.parse(this.schema, this.keyspace).build()));
         this.loader = new SSTableLoader(this.outputDir, externalClient, new CqlBulkRecordWriter.NullOutputHandler()) {
            public void onSuccess(StreamState finalState) {
               if(CqlBulkRecordWriter.this.deleteSrc) {
                  FileUtils.deleteRecursive(CqlBulkRecordWriter.this.outputDir);
               }

            }
         };
      }

   }

   public void write(Object key, List<ByteBuffer> values) throws IOException {
      this.prepareWriter();

      try {
         ((CQLSSTableWriter)this.writer).rawAddRow(values);
         if(null != this.progress) {
            this.progress.progress();
         }

         if(null != this.context) {
            HadoopCompat.progress(this.context);
         }

      } catch (InvalidRequestException var4) {
         throw new IOException("Error adding row with key: " + key, var4);
      }
   }

   private File getTableDirectory() throws IOException {
      File dir = new File(String.format("%s%s%s%s%s-%s", new Object[]{this.getOutputLocation(), File.separator, this.keyspace, File.separator, this.table, UUID.randomUUID().toString()}));
      if(!dir.exists() && !dir.mkdirs()) {
         throw new IOException("Failed to created output directory: " + dir);
      } else {
         return dir;
      }
   }

   public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      this.close();
   }

   /** @deprecated */
   @Deprecated
   public void close(Reporter reporter) throws IOException {
      this.close();
   }

   private void close() throws IOException {
      if(this.writer != null) {
         this.writer.close();
         StreamResultFuture future = this.loader.stream(this.ignores, new StreamEventHandler[0]);

         while(true) {
            try {
               future.get(1000L, TimeUnit.MILLISECONDS);
               break;
            } catch (TimeoutException | ExecutionException var3) {
               if(null != this.progress) {
                  this.progress.progress();
               }

               if(null != this.context) {
                  HadoopCompat.progress(this.context);
               }
            } catch (InterruptedException var4) {
               throw new IOException(var4);
            }
         }

         if(this.loader.getFailedHosts().size() > 0) {
            if(this.loader.getFailedHosts().size() > this.maxFailures) {
               throw new IOException("Too many hosts failed: " + this.loader.getFailedHosts());
            }

            this.logger.warn("Some hosts failed: {}", this.loader.getFailedHosts());
         }
      }

   }

   public static class NullOutputHandler implements OutputHandler {
      public NullOutputHandler() {
      }

      public void output(String msg) {
      }

      public void debug(String msg) {
      }

      public void warn(String msg) {
      }

      public void warn(String msg, Throwable th) {
      }
   }

   public static class ExternalClient extends NativeSSTableLoaderClient {
      public ExternalClient(Configuration conf) {
         super(resolveHostAddresses(conf), CqlConfigHelper.getOutputNativePort(conf), ConfigHelper.getOutputKeyspaceUserName(conf), ConfigHelper.getOutputKeyspacePassword(conf), (SSLOptions)CqlConfigHelper.getSSLOptions(conf).orNull());
      }

      private static Collection<InetAddress> resolveHostAddresses(Configuration conf) {
         Set<InetAddress> addresses = new HashSet();
         String[] var2 = ConfigHelper.getOutputInitialAddress(conf).split(",");
         int var3 = var2.length;

         for(int var4 = 0; var4 < var3; ++var4) {
            String host = var2[var4];

            try {
               addresses.add(InetAddress.getByName(host));
            } catch (UnknownHostException var7) {
               throw new RuntimeException(var7);
            }
         }

         return addresses;
      }
   }
}
