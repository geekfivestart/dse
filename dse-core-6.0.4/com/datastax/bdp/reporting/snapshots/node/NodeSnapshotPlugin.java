package com.datastax.bdp.reporting.snapshots.node;

import com.datastax.bdp.cassandra.metrics.PerformanceObjectsPlugin;
import com.datastax.bdp.plugin.DsePlugin;
import com.datastax.bdp.plugin.PerformanceObjectsController;
import com.datastax.bdp.plugin.ThreadPoolPlugin;
import com.datastax.bdp.reporting.snapshots.AbstractScheduledPlugin;
import com.datastax.bdp.server.system.BatchlogInfoProvider;
import com.datastax.bdp.server.system.CacheInfoProvider;
import com.datastax.bdp.server.system.ClientRequestMetricsProvider;
import com.datastax.bdp.server.system.CommitLogInfoProvider;
import com.datastax.bdp.server.system.CompactionInfoProvider;
import com.datastax.bdp.server.system.GCInfoProvider;
import com.datastax.bdp.server.system.MessagingInfoProvider;
import com.datastax.bdp.server.system.StorageInfoProvider;
import com.datastax.bdp.server.system.StreamInfoProvider;
import com.datastax.bdp.server.system.SystemResourcesInfoProvider;
import com.datastax.bdp.server.system.ThreadPoolStatsProvider;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.RuntimeMXBean;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@DsePlugin(
   dependsOn = {PerformanceObjectsPlugin.class, ThreadPoolPlugin.class}
)
public class NodeSnapshotPlugin extends AbstractScheduledPlugin<PerformanceObjectsController.ClusterSummaryStatsBean> {
   private NodeInfoWriter writer;
   private static final Logger logger = LoggerFactory.getLogger(NodeSnapshotPlugin.class);

   @Inject
   public NodeSnapshotPlugin(PerformanceObjectsController.ClusterSummaryStatsBean bean, ThreadPoolPlugin threadPool) {
      super(threadPool, bean, true);
   }

   protected Runnable getTask() {
      return new NodeSnapshotPlugin.PeriodicUpdateTask();
   }

   protected int getInitialDelay() {
      return Integer.getInteger("dse.node_snapshot_scheduler_initial_delay", this.getRefreshPeriod()).intValue();
   }

   public void setupSchema() {
      this.writer = new NodeInfoWriter(this.nodeAddress, this.getTTL());
      this.writer.createTable();
   }

   private class PeriodicUpdateTask implements Runnable {
      private final MessagingInfoProvider messaging;
      private final StreamInfoProvider streams;
      private final StorageInfoProvider storage;
      private final BatchlogInfoProvider batchLog;
      private final CacheInfoProvider keyCache;
      private final CacheInfoProvider rowCache;
      private final CompactionInfoProvider compaction;
      private final ThreadPoolStatsProvider threadPoolStats;
      private final CommitLogInfoProvider commitLog;
      private final ClientRequestMetricsProvider clientRequests;
      private final GCInfoProvider gcInfo;
      private final SystemResourcesInfoProvider systemResources;
      private final RuntimeMXBean runtime;
      private final MemoryMXBean memory;

      private PeriodicUpdateTask() {
         this.messaging = new MessagingInfoProvider.JmxMessagingInfoProvider();
         this.streams = new StreamInfoProvider.JmxStreamInfoProvider();
         this.storage = new StorageInfoProvider.JmxStorageInfoProvider();
         this.batchLog = new BatchlogInfoProvider.JmxBatchlogInfoProvider();
         this.keyCache = new CacheInfoProvider.KeyCacheInfoProvider();
         this.rowCache = new CacheInfoProvider.RowCacheInfoProvider();
         this.compaction = new CompactionInfoProvider.JmxCompactionInfoProvider();
         this.threadPoolStats = new ThreadPoolStatsProvider.JMXThreadPoolProvider();
         this.commitLog = new CommitLogInfoProvider.JmxCommitLogInfoProvider();
         this.clientRequests = new ClientRequestMetricsProvider.JmxClientRequestMetricsProvider();
         this.gcInfo = new GCInfoProvider.JmxGCInfoProvider();
         this.systemResources = new SystemResourcesInfoProvider.DefaultProvider();
         this.runtime = ManagementFactory.getRuntimeMXBean();
         this.memory = ManagementFactory.getMemoryMXBean();
      }

      public void run() {
         try {
            NodeInfo.Builder builder = (new NodeInfo.Builder()).withLocalAddress(NodeSnapshotPlugin.this.nodeAddress).withSnitch(DatabaseDescriptor.getEndpointSnitch()).withBatchlogInfo(this.batchLog).withStorageService(this.storage).withStreams(this.streams).withMessaging(this.messaging).withKeyCache(this.keyCache).withRowCache(this.rowCache).withCompaction(this.compaction).withThreadPools(this.threadPoolStats).withCommitLog(this.commitLog).withClientRequests(this.clientRequests).withGcInfo(this.gcInfo).withSystemResources(this.systemResources).withRuntime(this.runtime).withMemory(this.memory);
            NodeSnapshotPlugin.this.writer.write(builder.build());
         } catch (RuntimeException var2) {
            NodeSnapshotPlugin.logger.debug("Error performing periodic update of node summary info", var2);
         }

      }
   }
}
