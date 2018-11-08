package org.apache.cassandra.hadoop.cql3;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TokenRange;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.HadoopCompat;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CqlRecordWriter extends RecordWriter<Map<String, ByteBuffer>, List<ByteBuffer>> implements org.apache.hadoop.mapred.RecordWriter<Map<String, ByteBuffer>, List<ByteBuffer>>, AutoCloseable {
   private static final Logger logger = LoggerFactory.getLogger(CqlRecordWriter.class);
   protected final Configuration conf;
   protected final int queueSize;
   protected final long batchThreshold;
   protected Progressable progressable;
   protected TaskAttemptContext context;
   private final CqlRecordWriter.NativeRingCache ringCache;
   protected final Map<InetAddress, CqlRecordWriter.RangeClient> clients;
   protected final ConcurrentHashMap<Session, PreparedStatement> preparedStatements;
   protected final String cql;
   protected List<ColumnMetadata> partitionKeyColumns;
   protected List<ColumnMetadata> clusterColumns;

   CqlRecordWriter(TaskAttemptContext context) throws IOException {
      this(HadoopCompat.getConfiguration(context));
      this.context = context;
   }

   CqlRecordWriter(Configuration conf, Progressable progressable) {
      this(conf);
      this.progressable = progressable;
   }

   CqlRecordWriter(Configuration conf) {
      this.preparedStatements = new ConcurrentHashMap();
      this.conf = conf;
      this.queueSize = conf.getInt("mapreduce.output.columnfamilyoutputformat.queue.size", 32 * FBUtilities.getAvailableProcessors());
      this.batchThreshold = conf.getLong("mapreduce.output.columnfamilyoutputformat.batch.threshold", 32L);
      this.clients = new HashMap();
      String keyspace = ConfigHelper.getOutputKeyspace(conf);

      try {
         Cluster cluster = CqlConfigHelper.getOutputCluster(ConfigHelper.getOutputInitialAddress(conf), conf);
         Throwable var4 = null;

         try {
            Metadata metadata = cluster.getMetadata();
            this.ringCache = new CqlRecordWriter.NativeRingCache(conf, metadata);
            TableMetadata tableMetadata = metadata.getKeyspace(Metadata.quote(keyspace)).getTable(ConfigHelper.getOutputColumnFamily(conf));
            this.clusterColumns = tableMetadata.getClusteringColumns();
            this.partitionKeyColumns = tableMetadata.getPartitionKey();
            String cqlQuery = CqlConfigHelper.getOutputCql(conf).trim();
            if(cqlQuery.toLowerCase(Locale.ENGLISH).startsWith("insert")) {
               throw new UnsupportedOperationException("INSERT with CqlRecordWriter is not supported, please use UPDATE/DELETE statement");
            }

            this.cql = this.appendKeyWhereClauses(cqlQuery);
         } catch (Throwable var16) {
            var4 = var16;
            throw var16;
         } finally {
            if(cluster != null) {
               if(var4 != null) {
                  try {
                     cluster.close();
                  } catch (Throwable var15) {
                     var4.addSuppressed(var15);
                  }
               } else {
                  cluster.close();
               }
            }

         }

      } catch (Exception var18) {
         throw new RuntimeException(var18);
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

   public void close() throws IOException {
      IOException clientException = null;
      Iterator var2 = this.clients.values().iterator();

      while(var2.hasNext()) {
         CqlRecordWriter.RangeClient client = (CqlRecordWriter.RangeClient)var2.next();

         try {
            client.close();
         } catch (IOException var5) {
            clientException = var5;
         }
      }

      if(clientException != null) {
         throw clientException;
      }
   }

   public void write(Map<String, ByteBuffer> keyColumns, List<ByteBuffer> values) throws IOException {
      TokenRange range = this.ringCache.getRange(this.getPartitionKey(keyColumns));
      InetAddress address = (InetAddress)this.ringCache.getEndpoints(range).get(0);
      CqlRecordWriter.RangeClient client = (CqlRecordWriter.RangeClient)this.clients.get(address);
      if(client == null) {
         client = new CqlRecordWriter.RangeClient(this.ringCache.getEndpoints(range));
         client.start();
         this.clients.put(address, client);
      }

      List<ByteBuffer> allValues = new ArrayList(values);
      Iterator var7 = this.partitionKeyColumns.iterator();

      ColumnMetadata column;
      while(var7.hasNext()) {
         column = (ColumnMetadata)var7.next();
         allValues.add(keyColumns.get(column.getName()));
      }

      var7 = this.clusterColumns.iterator();

      while(var7.hasNext()) {
         column = (ColumnMetadata)var7.next();
         allValues.add(keyColumns.get(column.getName()));
      }

      client.put(allValues);
      if(this.progressable != null) {
         this.progressable.progress();
      }

      if(this.context != null) {
         HadoopCompat.progress(this.context);
      }

   }

   private static void closeSession(Session session) {
      try {
         if(session != null) {
            session.getCluster().closeAsync();
         }
      } catch (Throwable var2) {
         logger.warn("Error closing connection", var2);
      }

   }

   private ByteBuffer getPartitionKey(Map<String, ByteBuffer> keyColumns) {
      ByteBuffer partitionKey;
      if(this.partitionKeyColumns.size() > 1) {
         ByteBuffer[] keys = new ByteBuffer[this.partitionKeyColumns.size()];

         for(int i = 0; i < keys.length; ++i) {
            keys[i] = (ByteBuffer)keyColumns.get(((ColumnMetadata)this.partitionKeyColumns.get(i)).getName());
         }

         partitionKey = CompositeType.build(keys);
      } else {
         partitionKey = (ByteBuffer)keyColumns.get(((ColumnMetadata)this.partitionKeyColumns.get(0)).getName());
      }

      return partitionKey;
   }

   private String appendKeyWhereClauses(String cqlQuery) {
      String keyWhereClause = "";

      Iterator var3;
      ColumnMetadata clusterColumn;
      for(var3 = this.partitionKeyColumns.iterator(); var3.hasNext(); keyWhereClause = keyWhereClause + String.format("%s = ?", new Object[]{keyWhereClause.isEmpty()?this.quote(clusterColumn.getName()):" AND " + this.quote(clusterColumn.getName())})) {
         clusterColumn = (ColumnMetadata)var3.next();
      }

      for(var3 = this.clusterColumns.iterator(); var3.hasNext(); keyWhereClause = keyWhereClause + " AND " + this.quote(clusterColumn.getName()) + " = ?") {
         clusterColumn = (ColumnMetadata)var3.next();
      }

      return cqlQuery + " WHERE " + keyWhereClause;
   }

   private String quote(String identifier) {
      return "\"" + identifier.replaceAll("\"", "\"\"") + "\"";
   }

   static class NativeRingCache {
      private final Map<TokenRange, Set<Host>> rangeMap;
      private final Metadata metadata;
      private final IPartitioner partitioner;

      public NativeRingCache(Configuration conf, Metadata metadata) {
         this.partitioner = ConfigHelper.getOutputPartitioner(conf);
         this.metadata = metadata;
         String keyspace = ConfigHelper.getOutputKeyspace(conf);
         this.rangeMap = (Map)metadata.getTokenRanges().stream().collect(Collectors.toMap((p) -> {
            return p;
         }, (p) -> {
            return metadata.getReplicas('"' + keyspace + '"', p);
         }));
      }

      public TokenRange getRange(ByteBuffer key) {
         Token t = this.partitioner.getToken(key);
         com.datastax.driver.core.Token driverToken = this.metadata.newToken(this.partitioner.getTokenFactory().toString(t));
         Iterator var4 = this.rangeMap.keySet().iterator();

         TokenRange range;
         do {
            if(!var4.hasNext()) {
               throw new RuntimeException("Invalid token information returned by describe_ring: " + this.rangeMap);
            }

            range = (TokenRange)var4.next();
         } while(!range.contains(driverToken));

         return range;
      }

      public List<InetAddress> getEndpoints(TokenRange range) {
         Set<Host> hostSet = (Set)this.rangeMap.get(range);
         List<InetAddress> addresses = new ArrayList(hostSet.size());
         Iterator var4 = hostSet.iterator();

         while(var4.hasNext()) {
            Host host = (Host)var4.next();
            addresses.add(host.getAddress());
         }

         return addresses;
      }
   }

   public class RangeClient extends Thread {
      protected final List<InetAddress> endpoints;
      protected Cluster cluster = null;
      protected final BlockingQueue<List<ByteBuffer>> queue;
      protected volatile boolean run;
      protected volatile IOException lastException;

      public RangeClient(List<InetAddress> this$0) {
         super("client-" + endpoints);
         this.queue = new ArrayBlockingQueue(CqlRecordWriter.this.queueSize);
         this.run = true;
         this.endpoints = endpoints;
      }

      public void put(List<ByteBuffer> value) throws IOException {
         while(this.lastException == null) {
            try {
               if(this.queue.offer(value, 100L, TimeUnit.MILLISECONDS)) {
                  return;
               }
            } catch (InterruptedException var3) {
               throw new AssertionError(var3);
            }
         }

         throw this.lastException;
      }

      public void run() {
         Session session = null;

         try {
            label150:
            while(this.run || !this.queue.isEmpty()) {
               List bindVariables;
               try {
                  bindVariables = (List)this.queue.take();
               } catch (InterruptedException var13) {
                  continue;
               }

               ListIterator iter = this.endpoints.listIterator();

               while(true) {
                  if(session != null) {
                     try {
                        int i = 0;
                        PreparedStatement statement = this.preparedStatement(session);

                        while(true) {
                           if(bindVariables == null) {
                              continue label150;
                           }

                           BoundStatement boundStatement = new BoundStatement(statement);

                           for(int columnPosition = 0; columnPosition < bindVariables.size(); ++columnPosition) {
                              boundStatement.setBytesUnsafe(columnPosition, (ByteBuffer)bindVariables.get(columnPosition));
                           }

                           session.execute(boundStatement);
                           ++i;
                           if((long)i >= CqlRecordWriter.this.batchThreshold) {
                              continue label150;
                           }

                           bindVariables = (List)this.queue.poll();
                        }
                     } catch (Exception var15) {
                        this.closeInternal();
                        if(!iter.hasNext()) {
                           this.lastException = new IOException(var15);
                           return;
                        }
                     }
                  }

                  try {
                     InetAddress address = (InetAddress)iter.next();
                     String host = address.getHostName();
                     this.cluster = CqlConfigHelper.getOutputCluster(host, CqlRecordWriter.this.conf);
                     CqlRecordWriter.closeSession(session);
                     session = this.cluster.connect();
                  } catch (Exception var14) {
                     if(this.canRetryDriverConnection(var14)) {
                        iter.previous();
                     }

                     this.closeInternal();
                     if(var14 instanceof AuthenticationException || var14 instanceof InvalidQueryException || !iter.hasNext()) {
                        this.lastException = new IOException(var14);
                        return;
                     }
                  }
               }
            }
         } finally {
            CqlRecordWriter.closeSession(session);
            this.closeInternal();
         }

      }

      private PreparedStatement preparedStatement(Session client) {
         PreparedStatement statement = (PreparedStatement)CqlRecordWriter.this.preparedStatements.get(client);
         if(statement == null) {
            PreparedStatement result;
            try {
               result = client.prepare(CqlRecordWriter.this.cql);
            } catch (NoHostAvailableException var5) {
               throw new RuntimeException("failed to prepare cql query " + CqlRecordWriter.this.cql, var5);
            }

            PreparedStatement previousId = (PreparedStatement)CqlRecordWriter.this.preparedStatements.putIfAbsent(client, result);
            statement = previousId == null?result:previousId;
         }

         return statement;
      }

      public void close() throws IOException {
         this.run = false;
         this.interrupt();

         try {
            this.join();
         } catch (InterruptedException var2) {
            throw new AssertionError(var2);
         }

         if(this.lastException != null) {
            throw this.lastException;
         }
      }

      protected void closeInternal() {
         if(this.cluster != null) {
            this.cluster.close();
         }

      }

      private boolean canRetryDriverConnection(Exception e) {
         if(e instanceof DriverException && e.getMessage().contains("Connection thread interrupted")) {
            return true;
         } else {
            if(e instanceof NoHostAvailableException && ((NoHostAvailableException)e).getErrors().size() == 1) {
               Throwable cause = (Throwable)((NoHostAvailableException)e).getErrors().values().iterator().next();
               if(cause != null && cause.getCause() instanceof ClosedByInterruptException) {
                  return true;
               }
            }

            return false;
         }
      }
   }
}
