package org.apache.cassandra.metrics;

import com.codahale.metrics.Gauge;
import java.util.EnumMap;
import org.apache.cassandra.concurrent.TPCMetrics;
import org.apache.cassandra.concurrent.TPCTaskType;

public class TPCTotalMetrics {
   public final Gauge<Integer> activeTasks;
   public final Gauge<Long> completedTasks;
   public final Gauge<Integer> pendingTasks;
   public final Gauge<Long> blockedTasks;
   public final Gauge<Long> backpressureCountedTasks;
   public final Gauge<Long> backpressureDelayedTasks;
   public final TPCMetrics metrics;
   private final MetricNameFactory factory;
   private final EnumMap<TPCTaskType, TPCStageMetrics> stages;

   public TPCTotalMetrics(TPCMetrics metrics, String path, String poolPrefix) {
      this.metrics = metrics;
      this.factory = new ThreadPoolMetricNameFactory("ThreadPools", path, poolPrefix);
      this.activeTasks = (Gauge)CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName("ActiveTasks"), new Gauge<Integer>() {
         public Integer getValue() {
            return Integer.valueOf(TPCTotalMetrics.this.getActiveTotal());
         }
      });
      this.completedTasks = (Gauge)CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName("CompletedTasks"), new Gauge<Long>() {
         public Long getValue() {
            return Long.valueOf(TPCTotalMetrics.this.getCompletedTotal());
         }
      });
      this.pendingTasks = (Gauge)CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName("PendingTasks"), new Gauge<Integer>() {
         public Integer getValue() {
            return Integer.valueOf(TPCTotalMetrics.this.getPendingTotal());
         }
      });
      this.blockedTasks = (Gauge)CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName("TotalBlockedTasksGauge"), new Gauge<Long>() {
         public Long getValue() {
            return Long.valueOf(TPCTotalMetrics.this.getBlockedTotal());
         }
      });
      this.backpressureCountedTasks = (Gauge)CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName("TotalBackpressureCountedTasksGauge"), new Gauge<Long>() {
         public Long getValue() {
            return Long.valueOf(TPCTotalMetrics.this.getBackpressureCountedTotal());
         }
      });
      this.backpressureDelayedTasks = (Gauge)CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName("TotalBackpressureDelayedTasksGauge"), new Gauge<Long>() {
         public Long getValue() {
            return Long.valueOf(TPCTotalMetrics.this.getBackpressureDelayedTotal());
         }
      });
      this.stages = new EnumMap(TPCTaskType.class);
      TPCTaskType[] var5 = TPCTaskType.values();
      int var6 = var5.length;

      for(int var7 = 0; var7 < var6; ++var7) {
         TPCTaskType s = var5[var7];
         this.stages.put(s, new TPCStageMetrics(metrics, s, path, poolPrefix));
      }

   }

   private int getActiveTotal() {
      long active = 0L;
      TPCTaskType[] var3 = TPCTaskType.values();
      int var4 = var3.length;

      for(int var5 = 0; var5 < var4; ++var5) {
         TPCTaskType s = var3[var5];
         if(s.includedInTotals()) {
            active += this.metrics.activeTaskCount(s);
         }
      }

      return (int)active;
   }

   private long getCompletedTotal() {
      long completed = 0L;
      TPCTaskType[] var3 = TPCTaskType.values();
      int var4 = var3.length;

      for(int var5 = 0; var5 < var4; ++var5) {
         TPCTaskType s = var3[var5];
         if(s.includedInTotals()) {
            completed += this.metrics.completedTaskCount(s);
         }
      }

      return completed;
   }

   private int getPendingTotal() {
      long pending = 0L;
      TPCTaskType[] var3 = TPCTaskType.values();
      int var4 = var3.length;

      for(int var5 = 0; var5 < var4; ++var5) {
         TPCTaskType s = var3[var5];
         if(s.includedInTotals()) {
            pending += this.metrics.pendingTaskCount(s);
         }
      }

      return (int)pending;
   }

   private long getBlockedTotal() {
      long blocked = 0L;
      TPCTaskType[] var3 = TPCTaskType.values();
      int var4 = var3.length;

      for(int var5 = 0; var5 < var4; ++var5) {
         TPCTaskType s = var3[var5];
         if(s.includedInTotals()) {
            blocked += this.metrics.blockedTaskCount(s);
         }
      }

      return blocked;
   }

   private long getBackpressureCountedTotal() {
      return this.metrics.backpressureCountedTotalTaskCount();
   }

   private long getBackpressureDelayedTotal() {
      return this.metrics.backpressureDelayedTaskCount();
   }

   public void release() {
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("ActiveTasks"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("CompletedTasks"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("PendingTasks"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("TotalBlockedTasksGauge"));
      TPCTaskType[] var1 = TPCTaskType.values();
      int var2 = var1.length;

      for(int var3 = 0; var3 < var2; ++var3) {
         TPCTaskType s = var1[var3];
         ((TPCStageMetrics)this.stages.get(s)).release();
      }

   }
}
