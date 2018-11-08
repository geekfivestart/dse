package com.datastax.bdp.cassandra.metrics;

import com.datastax.bdp.leasemanager.LeaseMetricsWriter;
import com.datastax.bdp.leasemanager.LeasePlugin;
import com.datastax.bdp.plugin.DsePlugin;
import com.datastax.bdp.plugin.ThreadPoolPlugin;
import com.datastax.bdp.plugin.bean.LeaseMetricsBean;
import com.datastax.bdp.reporting.snapshots.AbstractScheduledPlugin;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@DsePlugin(
   dependsOn = {PerformanceObjectsPlugin.class, ThreadPoolPlugin.class, LeasePlugin.class}
)
public class LeaseMetricsPlugin extends AbstractScheduledPlugin<LeaseMetricsBean> {
   private static final Logger logger = LoggerFactory.getLogger(LeaseMetricsPlugin.class);
   protected LeasePlugin leasePlugin;
   protected LeaseMetricsWriter writer;

   @Inject
   public LeaseMetricsPlugin(LeaseMetricsBean mbean, ThreadPoolPlugin threadPool, LeasePlugin leasePlugin) {
      super(threadPool, mbean, true);
      this.leasePlugin = leasePlugin;
      this.writer = new LeaseMetricsWriter();
   }

   public void setupSchema() {
      this.writer.createTable();
   }

   protected Runnable getTask() {
      return () -> {
         try {
            this.leasePlugin.getMetrics().forEach((m) -> {
               this.writer.write(m);
            });
         } catch (Exception var2) {
            logger.warn("Error while storing Lease metrics to Cassandra", var2);
         }

      };
   }
}
