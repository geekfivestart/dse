package org.apache.cassandra.metrics;

import com.codahale.metrics.Gauge;
import org.apache.cassandra.concurrent.TPCMetrics;
import org.apache.cassandra.concurrent.TPCTaskType;

public class TPCStageMetrics {
   public final Gauge<Integer> activeTasks;
   public final Gauge<Long> completedTasks;
   public final Gauge<Integer> pendingTasks;
   public final Gauge<Long> blockedTasks;
   public final TPCMetrics metrics;
   public final TPCTaskType stage;
   private final MetricNameFactory factory;

   public TPCStageMetrics(final TPCMetrics metrics, final TPCTaskType taskType, String path, String poolPrefix) {
      this.metrics = metrics;
      this.stage = taskType;
      String poolName = poolPrefix + "/" + taskType.loggedEventName;
      this.factory = new ThreadPoolMetricNameFactory("ThreadPools", path, poolName);
      this.activeTasks = (Gauge)CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName("ActiveTasks"), new Gauge<Integer>() {
         public Integer getValue() {
            return Integer.valueOf((int)metrics.activeTaskCount(taskType));
         }
      });
      this.completedTasks = (Gauge)CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName("CompletedTasks"), new Gauge<Long>() {
         public Long getValue() {
            return Long.valueOf(metrics.completedTaskCount(taskType));
         }
      });
      if(taskType.pendable()) {
         this.pendingTasks = (Gauge)CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName("PendingTasks"), new Gauge<Integer>() {
            public Integer getValue() {
               return Integer.valueOf((int)metrics.pendingTaskCount(taskType));
            }
         });
         this.blockedTasks = (Gauge)CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName("TotalBlockedTasksGauge"), new Gauge<Long>() {
            public Long getValue() {
               return Long.valueOf(metrics.blockedTaskCount(taskType));
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
