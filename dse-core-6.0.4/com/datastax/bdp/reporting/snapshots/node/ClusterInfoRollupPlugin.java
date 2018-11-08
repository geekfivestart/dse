package com.datastax.bdp.reporting.snapshots.node;

import com.datastax.bdp.cassandra.cql3.StatementUtils;
import com.datastax.bdp.cassandra.metrics.PerformanceObjectsPlugin;
import com.datastax.bdp.leasemanager.SmallExclusiveTasksPlugin;
import com.datastax.bdp.plugin.DsePlugin;
import com.datastax.bdp.plugin.PerformanceObjectsController;
import com.datastax.bdp.plugin.ThreadPoolPlugin;
import com.datastax.bdp.plugin.bean.SnapshotInfoBean;
import com.datastax.bdp.reporting.snapshots.AbstractScheduledPlugin;
import com.datastax.bdp.util.QueryProcessorUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.UnmodifiableIterator;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.Iterator;
import java.util.Random;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.UntypedResultSet.Row;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.service.QueryState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@DsePlugin(
   dependsOn = {PerformanceObjectsPlugin.class, SmallExclusiveTasksPlugin.class, NodeSnapshotPlugin.class, ThreadPoolPlugin.class}
)
public class ClusterInfoRollupPlugin extends AbstractScheduledPlugin<SnapshotInfoBean> {
   private static final String SELECT_NODE_SNAPSHOT = String.format("SELECT * FROM %s.%s", new Object[]{"dse_perf", "node_snapshot"});
   private static final Logger logger = LoggerFactory.getLogger(ClusterInfoRollupPlugin.class);
   private DatacenterInfoWriter dcWriter;
   private ClusterInfoWriter clusterWriter;
   private SmallExclusiveTasksPlugin exclusiveTasksPlugin;

   @Inject
   public ClusterInfoRollupPlugin(PerformanceObjectsController.ClusterSummaryStatsBean bean, ThreadPoolPlugin threadPool, SmallExclusiveTasksPlugin exclusiveTasksPlugin) {
      super(threadPool, bean, true);
      this.exclusiveTasksPlugin = exclusiveTasksPlugin;
   }

   public void setupSchema() {
      this.dcWriter = new DatacenterInfoWriter(this.nodeAddress, this.getTTL() * 3);
      this.clusterWriter = new ClusterInfoWriter(this.nodeAddress, this.getTTL() * 3, DatabaseDescriptor.getClusterName());
      this.clusterWriter.createTable();
      this.dcWriter.createTable();
   }

   protected Runnable getTask() {
      return () -> {
         this.exclusiveTasksPlugin.executeIfLeader(this::doRollup, false, false);
      };
   }

   protected int getInitialDelay() {
      int delay = Integer.getInteger("dse.cluster_info_rollup_scheduler_initial_delay", -1).intValue();
      return delay >= 0?delay:1 + (new Random()).nextInt(10);
   }

   private void doRollup() {
      try {
         ClusterInfo cluster = this.readNodeSnapshots();
         Iterator var2 = cluster.iterator();

         while(var2.hasNext()) {
            DatacenterInfo dcInfo = (DatacenterInfo)var2.next();
            this.dcWriter.write(dcInfo);
         }

         this.clusterWriter.write(cluster);
      } catch (RuntimeException var4) {
         logger.warn("Caught exception while rolling up cluster info", var4);
      }

   }

   private ClusterInfo readNodeSnapshots() {
      ClusterInfo cluster = new ClusterInfo();

      try {
         CQLStatement stmt = StatementUtils.prepareStatementBlocking(SELECT_NODE_SNAPSHOT, QueryState.forInternalCalls());
         UntypedResultSet rows = QueryProcessorUtil.processPreparedSelect(stmt, ConsistencyLevel.ONE);
         UnmodifiableIterator var4 = ImmutableList.copyOf(rows.iterator()).iterator();

         while(var4.hasNext()) {
            Row row = (Row)var4.next();
            NodeInfo nodeInfo = NodeInfo.fromRow(row);
            logger.debug("Read snapshot of node {}", nodeInfo.address);
            cluster.addNode(nodeInfo);
         }
      } catch (RuntimeException var7) {
         logger.info("Error fetching node summary info - skipping: {}", var7.getMessage());
      }

      return cluster;
   }
}
