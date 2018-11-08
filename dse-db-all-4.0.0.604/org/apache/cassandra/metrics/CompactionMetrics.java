package org.apache.cassandra.metrics;

import com.codahale.metrics.Gauge;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;

public class CompactionMetrics implements CompactionManager.CompactionExecutorStatsCollector {
   public static final MetricNameFactory factory = new DefaultNameFactory("Compaction");
   private static final Set<CompactionInfo.Holder> compactions = Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap()));
   public final Gauge<Integer> pendingTasks;
   public final Gauge<Map<String, Map<String, Integer>>> pendingTasksByTableName;
   public final Gauge<Long> completedTasks;
   public final Meter totalCompactionsCompleted;
   public final Counter bytesCompacted;
   public final Counter compactionsReduced;
   public final Counter sstablesDropppedFromCompactions;
   public final Counter compactionsAborted;

   public CompactionMetrics(final ThreadPoolExecutor... collectors) {
      this.pendingTasks = (Gauge)CassandraMetricsRegistry.Metrics.register(factory.createMetricName("PendingTasks"), new Gauge<Integer>() {
         public Integer getValue() {
            int n = 0;
            Iterator var2 = Schema.instance.getKeyspaces().iterator();

            while(var2.hasNext()) {
               String keyspaceName = (String)var2.next();

               ColumnFamilyStore cfs;
               for(Iterator var4 = Keyspace.open(keyspaceName).getColumnFamilyStores().iterator(); var4.hasNext(); n += cfs.getCompactionStrategyManager().getEstimatedRemainingTasks()) {
                  cfs = (ColumnFamilyStore)var4.next();
               }
            }

            return Integer.valueOf(n + CompactionMetrics.compactions.size());
         }
      });
      this.pendingTasksByTableName = (Gauge)CassandraMetricsRegistry.Metrics.register(factory.createMetricName("PendingTasksByTableName"), new Gauge<Map<String, Map<String, Integer>>>() {
         public Map<String, Map<String, Integer>> getValue() {
            Map<String, Map<String, Integer>> resultMap = new HashMap();
            Iterator var2 = Schema.instance.getKeyspaces().iterator();

            while(var2.hasNext()) {
               String keyspaceName = (String)var2.next();
               Iterator var4 = Keyspace.open(keyspaceName).getColumnFamilyStores().iterator();

               while(var4.hasNext()) {
                  ColumnFamilyStore cfs = (ColumnFamilyStore)var4.next();
                  int taskNumber = cfs.getCompactionStrategyManager().getEstimatedRemainingTasks();
                  if(taskNumber > 0) {
                     if(!resultMap.containsKey(keyspaceName)) {
                        resultMap.put(keyspaceName, new HashMap());
                     }

                     ((Map)resultMap.get(keyspaceName)).put(cfs.getTableName(), Integer.valueOf(taskNumber));
                  }
               }
            }

            var2 = CompactionMetrics.compactions.iterator();

            while(var2.hasNext()) {
               CompactionInfo.Holder compaction = (CompactionInfo.Holder)var2.next();
               TableMetadata metaData = compaction.getCompactionInfo().getTableMetadata();
               if(metaData != null) {
                  if(!resultMap.containsKey(metaData.keyspace)) {
                     resultMap.put(metaData.keyspace, new HashMap());
                  }

                  Map<String, Integer> tableNameToCountMap = (Map)resultMap.get(metaData.keyspace);
                  if(tableNameToCountMap.containsKey(metaData.name)) {
                     tableNameToCountMap.put(metaData.name, Integer.valueOf(((Integer)tableNameToCountMap.get(metaData.name)).intValue() + 1));
                  } else {
                     tableNameToCountMap.put(metaData.name, Integer.valueOf(1));
                  }
               }
            }

            return resultMap;
         }
      });
      this.completedTasks = (Gauge)CassandraMetricsRegistry.Metrics.register(factory.createMetricName("CompletedTasks"), new Gauge<Long>() {
         public Long getValue() {
            long completedTasks = 0L;
            ThreadPoolExecutor[] var3 = collectors;
            int var4 = var3.length;

            for(int var5 = 0; var5 < var4; ++var5) {
               ThreadPoolExecutor collector = var3[var5];
               completedTasks += collector.getCompletedTaskCount();
            }

            return Long.valueOf(completedTasks);
         }
      });
      this.totalCompactionsCompleted = CassandraMetricsRegistry.Metrics.meter(factory.createMetricName("TotalCompactionsCompleted"));
      this.bytesCompacted = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("BytesCompacted"));
      this.compactionsReduced = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("CompactionsReduced"));
      this.sstablesDropppedFromCompactions = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("SSTablesDroppedFromCompaction"));
      this.compactionsAborted = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("CompactionsAborted"));
   }

   public void beginCompaction(CompactionInfo.Holder ci) {
      compactions.add(ci);
   }

   public void finishCompaction(CompactionInfo.Holder ci) {
      compactions.remove(ci);
      this.bytesCompacted.inc(ci.getCompactionInfo().getTotal());
      this.totalCompactionsCompleted.mark();
   }

   public static List<CompactionInfo.Holder> getCompactions() {
      return new ArrayList(compactions);
   }
}
