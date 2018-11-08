package org.apache.cassandra.metrics;

import com.codahale.metrics.Gauge;
import org.apache.cassandra.concurrent.TPCMetrics;
import org.apache.cassandra.concurrent.TPCTaskType;

public class TPCAggregatedStageMetrics {
   public final Gauge<Integer> activeTasks;
   public final Gauge<Long> completedTasks;
   public final Gauge<Integer> pendingTasks;
   public final Gauge<Long> blockedTasks;
   public final TPCMetrics[] metrics;
   public final TPCTaskType stage;
   private final MetricNameFactory factory;

   public TPCAggregatedStageMetrics(final TPCMetrics[] metrics, final TPCTaskType stage, String path, String poolPrefix) {
      this.metrics = metrics;
      this.stage = stage;
      String poolName = poolPrefix + "/" + stage.loggedEventName;
      this.factory = new ThreadPoolMetricNameFactory("ThreadPools", path, poolName);
      this.activeTasks = (Gauge)CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName("ActiveTasks"), new Gauge<Integer>() {
         public Integer getValue() {
            long v = 0L;

            for(int i = 0; i < metrics.length; ++i) {
               v += metrics[i].activeTaskCount(stage);
            }

            return Integer.valueOf((int)v);
         }
      });
      this.completedTasks = (Gauge)CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName("CompletedTasks"), new Gauge<Long>() {
         public Long getValue() {
            long v = 0L;

            for(int i = 0; i < metrics.length; ++i) {
               v += metrics[i].completedTaskCount(stage);
            }

            return Long.valueOf(v);
         }
      });
      if(stage.pendable()) {
         this.pendingTasks = (Gauge)CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName("PendingTasks"), new Gauge<Integer>() {
            public Integer getValue() {
               long v = 0L;

               for(int i = 0; i < metrics.length; ++i) {
                  v += metrics[i].pendingTaskCount(stage);
               }

               return Integer.valueOf((int)v);
            }
         });
         this.blockedTasks = (Gauge)CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName("TotalBlockedTasksGauge"), new Gauge<Long>() {
            public Long getValue() {
               long v = 0L;

               for(int i = 0; i < metrics.length; ++i) {
                  v += metrics[i].blockedTaskCount(stage);
               }

               return Long.valueOf(v);
            }
         });
      } else {
         this.pendingTasks = null;
         this.blockedTasks = null;
      }

   }

   public void release() {
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("ActiveTasks"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("CompletedTasks"));
      if(this.pendingTasks != null) {
         CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("PendingTasks"));
         CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("TotalBlockedTasksGauge"));
      }

   }
}
