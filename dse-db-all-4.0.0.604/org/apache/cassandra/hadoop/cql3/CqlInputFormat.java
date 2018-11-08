package org.apache.cassandra.hadoop.cql3;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TokenRange;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.hadoop.ColumnFamilySplit;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.HadoopCompat;
import org.apache.cassandra.hadoop.ReporterWrapper;
import org.apache.cassandra.utils.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CqlInputFormat extends InputFormat<Long, Row> implements org.apache.hadoop.mapred.InputFormat<Long, Row> {
   public static final String MAPRED_TASK_ID = "mapred.task.id";
   private static final Logger logger = LoggerFactory.getLogger(CqlInputFormat.class);
   private String keyspace;
   private String cfName;
   private IPartitioner partitioner;

   public CqlInputFormat() {
   }

   public RecordReader<Long, Row> getRecordReader(InputSplit split, JobConf jobConf, Reporter reporter) throws IOException {
      TaskAttemptContext tac = HadoopCompat.newMapContext(jobConf, TaskAttemptID.forName(jobConf.get("mapred.task.id")), (org.apache.hadoop.mapreduce.RecordReader)null, (RecordWriter)null, (OutputCommitter)null, new ReporterWrapper(reporter), (org.apache.hadoop.mapreduce.InputSplit)null);
      CqlRecordReader recordReader = new CqlRecordReader();
      recordReader.initialize((org.apache.hadoop.mapreduce.InputSplit)split, tac);
      return recordReader;
   }

   public org.apache.hadoop.mapreduce.RecordReader<Long, Row> createRecordReader(org.apache.hadoop.mapreduce.InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
      return new CqlRecordReader();
   }

   protected void validateConfiguration(Configuration conf) {
      if(ConfigHelper.getInputKeyspace(conf) != null && ConfigHelper.getInputColumnFamily(conf) != null) {
         if(ConfigHelper.getInputInitialAddress(conf) == null) {
            throw new UnsupportedOperationException("You must set the initial output address to a Cassandra node with setInputInitialAddress");
         } else if(ConfigHelper.getInputPartitioner(conf) == null) {
            throw new UnsupportedOperationException("You must set the Cassandra partitioner class with setInputPartitioner");
         }
      } else {
         throw new UnsupportedOperationException("you must set the keyspace and table with setInputColumnFamily()");
      }
   }

   public List<org.apache.hadoop.mapreduce.InputSplit> getSplits(JobContext context) throws IOException {
      Configuration conf = HadoopCompat.getConfiguration(context);
      this.validateConfiguration(conf);
      this.keyspace = ConfigHelper.getInputKeyspace(conf);
      this.cfName = ConfigHelper.getInputColumnFamily(conf);
      this.partitioner = ConfigHelper.getInputPartitioner(conf);
      logger.trace("partitioner is {}", this.partitioner);
      ExecutorService executor = new ThreadPoolExecutor(0, 128, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue());
      ArrayList splits = new ArrayList();

      try {
         Cluster cluster = CqlConfigHelper.getInputCluster(ConfigHelper.getInputInitialAddress(conf).split(","), conf);
         Throwable var6 = null;

         try {
            Session session = cluster.connect();
            Throwable var8 = null;

            try {
               List<Future<List<org.apache.hadoop.mapreduce.InputSplit>>> splitfutures = new ArrayList();
               Pair<String, String> jobKeyRange = ConfigHelper.getInputKeyRange(conf);
               Range<Token> jobRange = null;
               if(jobKeyRange != null) {
                  jobRange = new Range(this.partitioner.getTokenFactory().fromString((String)jobKeyRange.left), this.partitioner.getTokenFactory().fromString((String)jobKeyRange.right));
               }

               Metadata metadata = cluster.getMetadata();
               Map<TokenRange, Set<Host>> masterRangeNodes = this.getRangeMap(this.keyspace, metadata);
               Iterator var14 = masterRangeNodes.keySet().iterator();

               label442:
               while(true) {
                  while(var14.hasNext()) {
                     TokenRange range = (TokenRange)var14.next();
                     if(jobRange == null) {
                        splitfutures.add(executor.submit(new CqlInputFormat.SplitCallable(range, (Set)masterRangeNodes.get(range), conf, session)));
                     } else {
                        TokenRange jobTokenRange = this.rangeToTokenRange(metadata, jobRange);
                        if(range.intersects(jobTokenRange)) {
                           Iterator var17 = range.intersectWith(jobTokenRange).iterator();

                           while(var17.hasNext()) {
                              TokenRange intersection = (TokenRange)var17.next();
                              splitfutures.add(executor.submit(new CqlInputFormat.SplitCallable(intersection, (Set)masterRangeNodes.get(range), conf, session)));
                           }
                        }
                     }
                  }

                  var14 = splitfutures.iterator();

                  while(true) {
                     if(!var14.hasNext()) {
                        break label442;
                     }

                     Future futureInputSplits = (Future)var14.next();

                     try {
                        splits.addAll((Collection)futureInputSplits.get());
                     } catch (Exception var55) {
                        throw new IOException("Could not get input splits", var55);
                     }
                  }
               }
            } catch (Throwable var56) {
               var8 = var56;
               throw var56;
            } finally {
               if(session != null) {
                  if(var8 != null) {
                     try {
                        session.close();
                     } catch (Throwable var54) {
                        var8.addSuppressed(var54);
                     }
                  } else {
                     session.close();
                  }
               }

            }
         } catch (Throwable var58) {
            var6 = var58;
            throw var58;
         } finally {
            if(cluster != null) {
               if(var6 != null) {
                  try {
                     cluster.close();
                  } catch (Throwable var53) {
                     var6.addSuppressed(var53);
                  }
               } else {
                  cluster.close();
               }
            }

         }
      } finally {
         executor.shutdownNow();
      }

      assert splits.size() > 0;

      Collections.shuffle(splits, new Random(System.nanoTime()));
      return splits;
   }

   private TokenRange rangeToTokenRange(Metadata metadata, Range<Token> range) {
      return metadata.newTokenRange(metadata.newToken(this.partitioner.getTokenFactory().toString((Token)range.left)), metadata.newToken(this.partitioner.getTokenFactory().toString((Token)range.right)));
   }

   private Map<TokenRange, Long> getSubSplits(String keyspace, String cfName, TokenRange range, Configuration conf, Session session) {
      int splitSize = ConfigHelper.getInputSplitSize(conf);
      int splitSizeMb = ConfigHelper.getInputSplitSizeInMb(conf);

      try {
         return this.describeSplits(keyspace, cfName, range, splitSize, splitSizeMb, session);
      } catch (Exception var9) {
         throw new RuntimeException(var9);
      }
   }

   private Map<TokenRange, Set<Host>> getRangeMap(String keyspace, Metadata metadata) {
      return (Map)metadata.getTokenRanges().stream().collect(Collectors.toMap((p) -> {
         return p;
      }, (p) -> {
         return metadata.getReplicas('"' + keyspace + '"', p);
      }));
   }

   private Map<TokenRange, Long> describeSplits(String keyspace, String table, TokenRange tokenRange, int splitSize, int splitSizeMb, Session session) {
      String query = String.format("SELECT mean_partition_size, partitions_count FROM %s.%s WHERE keyspace_name = ? AND table_name = ? AND range_start = ? AND range_end = ?", new Object[]{"system", "size_estimates"});
      ResultSet resultSet = session.execute(query, new Object[]{keyspace, table, tokenRange.getStart().toString(), tokenRange.getEnd().toString()});
      Row row = resultSet.one();
      long meanPartitionSize = 0L;
      long partitionCount = 0L;
      int splitCount = 0;
      if(row != null) {
         meanPartitionSize = row.getLong("mean_partition_size");
         partitionCount = row.getLong("partitions_count");
         splitCount = splitSizeMb > 0?(int)(meanPartitionSize * partitionCount / (long)splitSizeMb / 1024L / 1024L):(int)(partitionCount / (long)splitSize);
      }

      if(splitCount == 0) {
         Map<TokenRange, Long> wrappedTokenRange = new HashMap();
         wrappedTokenRange.put(tokenRange, Long.valueOf(128L));
         return wrappedTokenRange;
      } else {
         List<TokenRange> splitRanges = tokenRange.splitEvenly(splitCount);
         Map<TokenRange, Long> rangesWithLength = Maps.newHashMapWithExpectedSize(splitRanges.size());
         Iterator var17 = splitRanges.iterator();

         while(var17.hasNext()) {
            TokenRange range = (TokenRange)var17.next();
            rangesWithLength.put(range, Long.valueOf(partitionCount / (long)splitCount));
         }

         return rangesWithLength;
      }
   }

   public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
      TaskAttemptContext tac = HadoopCompat.newTaskAttemptContext(jobConf, new TaskAttemptID());
      List<org.apache.hadoop.mapreduce.InputSplit> newInputSplits = this.getSplits(tac);
      InputSplit[] oldInputSplits = new InputSplit[newInputSplits.size()];

      for(int i = 0; i < newInputSplits.size(); ++i) {
         oldInputSplits[i] = (ColumnFamilySplit)newInputSplits.get(i);
      }

      return oldInputSplits;
   }

   class SplitCallable implements Callable<List<org.apache.hadoop.mapreduce.InputSplit>> {
      private final TokenRange tokenRange;
      private final Set<Host> hosts;
      private final Configuration conf;
      private final Session session;

      public SplitCallable(TokenRange this$0, Set<Host> tr, Configuration hosts, Session conf) {
         this.tokenRange = tr;
         this.hosts = hosts;
         this.conf = conf;
         this.session = session;
      }

      public List<org.apache.hadoop.mapreduce.InputSplit> call() throws Exception {
         ArrayList<org.apache.hadoop.mapreduce.InputSplit> splits = new ArrayList();
         Map<TokenRange, Long> subSplits = CqlInputFormat.this.getSubSplits(CqlInputFormat.this.keyspace, CqlInputFormat.this.cfName, this.tokenRange, this.conf, this.session);
         String[] endpoints = new String[this.hosts.size()];
         int endpointIndex = 0;

         Host endpoint;
         for(Iterator var5 = this.hosts.iterator(); var5.hasNext(); endpoints[endpointIndex++] = endpoint.getAddress().getHostName()) {
            endpoint = (Host)var5.next();
         }

         boolean partitionerIsOpp = CqlInputFormat.this.partitioner instanceof OrderPreservingPartitioner || CqlInputFormat.this.partitioner instanceof ByteOrderedPartitioner;
         Iterator var13 = subSplits.entrySet().iterator();

         while(var13.hasNext()) {
            Entry<TokenRange, Long> subSplitEntry = (Entry)var13.next();
            List<TokenRange> ranges = ((TokenRange)subSplitEntry.getKey()).unwrap();
            Iterator var9 = ranges.iterator();

            while(var9.hasNext()) {
               TokenRange subrange = (TokenRange)var9.next();
               ColumnFamilySplit split = new ColumnFamilySplit(partitionerIsOpp?subrange.getStart().toString().substring(2):subrange.getStart().toString(), partitionerIsOpp?subrange.getEnd().toString().substring(2):subrange.getEnd().toString(), ((Long)subSplitEntry.getValue()).longValue(), endpoints);
               CqlInputFormat.logger.trace("adding {}", split);
               splits.add(split);
            }
         }

         return splits;
      }
   }
}
