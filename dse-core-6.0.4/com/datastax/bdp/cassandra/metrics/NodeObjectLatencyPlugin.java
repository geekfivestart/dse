package com.datastax.bdp.cassandra.metrics;

import com.datastax.bdp.plugin.DsePlugin;
import com.datastax.bdp.plugin.PerformanceObjectsController;
import com.datastax.bdp.plugin.ThreadPoolPlugin;
import com.datastax.bdp.reporting.snapshots.AbstractScheduledPlugin;
import com.datastax.bdp.system.PerformanceObjectsKeyspace;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@DsePlugin(
   dependsOn = {PerformanceObjectsPlugin.class, ThreadPoolPlugin.class}
)
public class NodeObjectLatencyPlugin extends AbstractScheduledPlugin<PerformanceObjectsController.ResourceLatencyTrackingBean> {
   private static final Logger logger = LoggerFactory.getLogger(NodeObjectLatencyPlugin.class);

   @Inject
   public NodeObjectLatencyPlugin(PerformanceObjectsController.ResourceLatencyTrackingBean mbean, ThreadPoolPlugin threadPool) {
      super(threadPool, mbean, true);
      logger.debug("Initializing data object io tracker plugin");
   }

   protected Runnable getTask() {
      return new NodeMetricsWriter();
   }

   public void setupSchema() {
      PerformanceObjectsKeyspace.maybeCreateTable("object_read_io_snapshot");
      PerformanceObjectsKeyspace.maybeCreateTable("object_write_io_snapshot");
      PerformanceObjectsKeyspace.maybeCreateTable("object_io");
   }
}
