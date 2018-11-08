package com.datastax.bdp.reporting.snapshots.histograms;

import com.datastax.bdp.cassandra.metrics.PerformanceObjectsPlugin;
import com.datastax.bdp.gms.DseVersionNotifier;
import com.datastax.bdp.plugin.DsePlugin;
import com.datastax.bdp.plugin.ThreadPoolPlugin;
import com.datastax.bdp.plugin.bean.HistogramDataMXBean;
import com.datastax.bdp.plugin.bean.HistogramDataTablesBean;
import com.datastax.bdp.plugin.bean.SnapshotInfoBean;
import com.datastax.bdp.reporting.CqlWriter;
import com.datastax.bdp.reporting.snapshots.AbstractScheduledPlugin;
import com.datastax.bdp.snitch.EndpointStateTracker;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.beans.PropertyChangeListener;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import javax.management.JMX;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@DsePlugin(
   dependsOn = {PerformanceObjectsPlugin.class, ThreadPoolPlugin.class}
)
public class HistogramInfoPlugin extends AbstractScheduledPlugin<SnapshotInfoBean> {
   public static volatile boolean isCollectingKeyspaceGlobalAndRange;
   private static final Logger logger = LoggerFactory.getLogger(HistogramInfoPlugin.class);
   private volatile List<Pair<CfsHistogramDataProvider, AbstractHistogramWriter>> histograms;
   private volatile List<Pair<KsHistogramDataProvider, CqlWriter<HistogramInfo>>> ksHistograms;
   private volatile DroppedMessagesWriter droppedMsgWriter;
   private final PropertyChangeListener retentionCountListener = (evt) -> {
      int ttl = this.getTTL();
      this.droppedMsgWriter.setTtl(ttl);
      this.histograms.forEach((pair) -> {
         ((AbstractHistogramWriter)pair.right).setTtl(ttl);
      });
      this.ksHistograms.forEach((pair) -> {
         ((CqlWriter)pair.right).setTtl(ttl);
      });
   };
   private volatile List<Pair<GlobalHistogramDataProvider, GlobalHistogramWriter>> globalHistograms;
   private static StorageServiceMBean storageService;

   @Inject
   public HistogramInfoPlugin(HistogramDataTablesBean mbean, ThreadPoolPlugin threadPool) {
      super(threadPool, mbean, true);
   }

   public void onActivate() {
      super.onActivate();
      this.getMbean().addPropertyChangeListener("retentionCount", this.retentionCountListener);
   }

   public void onPreDeactivate() {
      this.getMbean().removePropertyChangeListener("retentionCount", this.retentionCountListener);
      super.onPreDeactivate();
   }

   public void setupSchema() {
      int ttl = this.getTTL();

      try {
         storageService = (StorageServiceMBean)JMX.newMBeanProxy(ManagementFactory.getPlatformMBeanServer(), new ObjectName("org.apache.cassandra.db:type=StorageService"), StorageServiceMBean.class);
      } catch (MalformedObjectNameException var3) {
         throw new AssertionError("Storage service is not available", var3);
      }

      this.droppedMsgWriter = new DroppedMessagesWriter(this.nodeAddress, ttl);
      this.globalHistograms = ImmutableList.of(Pair.create(() -> {
         return TableMetrics.globalReadLatency.latency.getSnapshot().getValues();
      }, new GlobalHistogramWriter(this.nodeAddress, ttl, "read_latency_histograms_global")), Pair.create(() -> {
         return TableMetrics.globalWriteLatency.latency.getSnapshot().getValues();
      }, new GlobalHistogramWriter(this.nodeAddress, ttl, "write_latency_histograms_global")), Pair.create(() -> {
         return TableMetrics.globalRangeLatency.latency.getSnapshot().getValues();
      }, new GlobalHistogramWriter(this.nodeAddress, ttl, "range_latency_histograms_global")));
      this.ksHistograms = ImmutableList.of(Pair.create((ks) -> {
         return ks.metric.readLatency.latency.getSnapshot().getValues();
      }, new KeyspaceHistogramWriter(this.nodeAddress, ttl, "read_latency_histograms_ks")), Pair.create((ks) -> {
         return ks.metric.writeLatency.latency.getSnapshot().getValues();
      }, new KeyspaceHistogramWriter(this.nodeAddress, ttl, "write_latency_histograms_ks")), Pair.create((ks) -> {
         return ks.metric.rangeLatency.latency.getSnapshot().getValues();
      }, new KeyspaceHistogramWriter(this.nodeAddress, ttl, "range_latency_histograms_ks")), Pair.create((ks) -> {
         return ks.metric.sstablesPerReadHistogram.getSnapshot().getValues();
      }, new KeyspaceHistogramWriter(this.nodeAddress, ttl, "sstables_per_read_histograms_ks")));
      this.histograms = new ArrayList(Arrays.asList(new Pair[]{Pair.create((cfs) -> {
         return cfs.metric.readLatency.latency.getSnapshot().getValues();
      }, new ReadLatencyHistogramWriter(this.nodeAddress, ttl)), Pair.create((cfs) -> {
         return cfs.metric.writeLatency.latency.getSnapshot().getValues();
      }, new WriteLatencyHistogramWriter(this.nodeAddress, ttl)), Pair.create((cfs) -> {
         return (long[])cfs.metric.estimatedColumnCountHistogram.getValue();
      }, new CellCountHistogramWriter(this.nodeAddress, ttl)), Pair.create((cfs) -> {
         return (long[])cfs.metric.estimatedPartitionSizeHistogram.getValue();
      }, new PartitionSizeHistogramWriter(this.nodeAddress, ttl)), Pair.create((cfs) -> {
         return cfs.metric.sstablesPerReadHistogram.getSnapshot().getValues();
      }, new SSTablesPerReadHistogramWriter(this.nodeAddress, ttl))}));
      this.histograms.forEach((pair) -> {
         ((AbstractHistogramWriter)pair.right).createTable();
      });
      this.createGlobalTables(Pair.create((cfs) -> {
         return cfs.metric.rangeLatency.latency.getSnapshot().getValues();
      }, new RangeLatencyHistogramWriter(this.nodeAddress, ttl)));
      logger.info("Starting to collect keyspace and global statistics.");
      CassandraVersion var10000 = DseVersionNotifier.VERSION_51;
      DroppedMessagesWriter var10002 = this.droppedMsgWriter;
      this.droppedMsgWriter.getClass();
      EndpointStateTracker.runAtVersion(var10000, "Dropped message statistics won't be collected until all nodes are running 5.1.0 or higher", var10002::createTable, "Starting to collect dropped message statistics.");
   }

   private void createGlobalTables(Pair<CfsHistogramDataProvider, AbstractHistogramWriter> rangeLatencyHistogram) {
      ((AbstractHistogramWriter)rangeLatencyHistogram.right).createTable();
      this.histograms.add(rangeLatencyHistogram);
      this.globalHistograms.forEach((pair) -> {
         ((GlobalHistogramWriter)pair.right).createTable();
      });
      this.ksHistograms.forEach((pair) -> {
         ((CqlWriter)pair.right).createTable();
      });
      isCollectingKeyspaceGlobalAndRange = true;
   }

   protected Runnable getTask() {
      return new HistogramInfoPlugin.PeriodicUpdateTask();
   }

   public final int getTTL() {
      int retentionCount = ((HistogramDataMXBean)this.getMbean()).getRetentionCount();
      return this.getRefreshPeriod() / 1000 * retentionCount + 2;
   }

   @VisibleForTesting
   public class PeriodicUpdateTask implements Runnable {
      public PeriodicUpdateTask() {
      }

      public void run() {
         try {
            HistogramInfoPlugin.logger.debug("Running histogram writing task");
            if(HistogramInfoPlugin.isCollectingKeyspaceGlobalAndRange) {
               HistogramInfoPlugin.this.globalHistograms.forEach((pair) -> {
                  HistogramInfoPlugin.logger.debug("Processing {}", ((GlobalHistogramWriter)pair.right).getTableName());
                  ((GlobalHistogramWriter)pair.right).write(new HistogramInfo((String)null, (String)null, ((GlobalHistogramDataProvider)pair.left).getHistogramData()));
               });
               this.runSubtask(HistogramInfoPlugin.this.droppedMsgWriter);
            }

            Keyspace.all().forEach((keyspace) -> {
               if(!PerformanceObjectsPlugin.isUntracked(keyspace.getName())) {
                  if(HistogramInfoPlugin.isCollectingKeyspaceGlobalAndRange) {
                     HistogramInfoPlugin.this.ksHistograms.forEach((histogram) -> {
                        this.runSubtask(() -> {
                           HistogramInfoPlugin.logger.debug("Processing histogram data for keyspace {}", keyspace.getName());
                           ((CqlWriter)histogram.right).write(new HistogramInfo(keyspace.getName(), (String)null, ((KsHistogramDataProvider)histogram.left).getHistogramData(keyspace)));
                        });
                     });
                  }

                  keyspace.getColumnFamilyStores().forEach((cfs) -> {
                     HistogramInfoPlugin.this.histograms.forEach((histogram) -> {
                        this.runSubtask(() -> {
                           HistogramInfoPlugin.logger.debug("Processing histogram {} for data table {}.{}", new Object[]{((AbstractHistogramWriter)histogram.right).getClass().getName(), keyspace.getName(), cfs.getTableName()});
                           ((AbstractHistogramWriter)histogram.right).write(new HistogramInfo(keyspace.getName(), cfs.getTableName(), ((CfsHistogramDataProvider)histogram.left).getHistogramData(cfs), cfs.metric.droppedMutations.getCount()));
                        });
                     });
                  });
               }
            });
         } catch (RuntimeException var2) {
            HistogramInfoPlugin.logger.error("Error performing periodic update of histogram table", var2);
         }

      }

      private void runSubtask(Runnable task) {
         if(Boolean.getBoolean("dse.histo_table_writer_disable_concurrent_writes")) {
            task.run();
         } else {
            HistogramInfoPlugin.this.getThreadPool().submit(task);
         }

      }
   }
}
