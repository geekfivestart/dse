package com.datastax.bdp.server.system;

import javax.management.MBeanServer;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.metrics.ThreadPoolMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadPoolStats {
   private static final Logger logger = LoggerFactory.getLogger(ThreadPoolStats.class);
   private final MBeanServer server;
   private final ThreadPoolStats.Pool pool;

   public ThreadPoolStats(MBeanServer server, ThreadPoolStats.Pool pool) {
      this.server = server;
      this.pool = pool;
   }

   public String getName() {
      return this.pool.name;
   }

   public long getActiveTasks() {
      return this.getMetricValue("ActiveTasks");
   }

   public long getPendingTasks() {
      return this.getMetricValue("PendingTasks");
   }

   public long getCompletedTasks() {
      return this.getMetricValue("CompletedTasks");
   }

   public long getCurrentlyBlockedTasks() {
      return this.getMetricValue("CurrentlyBlockedTasks");
   }

   public long getTotalBlockedTasks() {
      return this.getMetricValue("TotalBlockedTasks");
   }

   private long getMetricValue(String metric) {
      Object value = ThreadPoolMetrics.getJmxMetric(this.server, this.pool.type, this.pool.name, metric);
      if(value instanceof Number) {
         return ((Number)value).longValue();
      } else {
         logger.debug("Could not get value for " + this.pool + "." + metric + " because ThreadPoolMetrics returned value of type " + value);
         return -1L;
      }
   }

   public static enum Pool {
      BACKGROUND_IO(Stage.BACKGROUND_IO.getJmxType(), Stage.BACKGROUND_IO.getJmxName()),
      READ_REPAIR(Stage.READ_REPAIR.getJmxType(), Stage.READ_REPAIR.getJmxName()),
      ANTI_ENTROPY(Stage.ANTI_ENTROPY.getJmxType(), Stage.ANTI_ENTROPY.getJmxName()),
      GOSSIP(Stage.GOSSIP.getJmxType(), Stage.GOSSIP.getJmxName()),
      INTERNAL_RESPONSE(Stage.INTERNAL_RESPONSE.getJmxType(), Stage.INTERNAL_RESPONSE.getJmxName()),
      MIGRATION(Stage.MIGRATION.getJmxType(), Stage.MIGRATION.getJmxName()),
      MISC(Stage.MISC.getJmxType(), Stage.MISC.getJmxName()),
      REQUEST_RESPONSE(Stage.REQUEST_RESPONSE.getJmxType(), Stage.REQUEST_RESPONSE.getJmxName()),
      MEMTABLE_FLUSH_WRITER("internal", "MemtableFlushWriter"),
      MEMTABLE_POST_FLUSH("internal", "MemtablePostFlush"),
      HINTED_HANDOFF("internal", "HintedHandoff");

      public final String type;
      public final String name;

      private Pool(String type, String name) {
         this.type = type;
         this.name = name;
      }
   }
}
