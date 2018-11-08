package org.apache.cassandra.metrics;

import com.codahale.metrics.Gauge;
import org.apache.cassandra.db.commitlog.AbstractCommitLogSegmentManager;
import org.apache.cassandra.db.commitlog.AbstractCommitLogService;

public class CommitLogMetrics {
   public static final MetricNameFactory factory = new DefaultNameFactory("CommitLog");
   public Gauge<Long> completedTasks;
   public Gauge<Long> pendingTasks;
   public Gauge<Long> totalCommitLogSize;
   public final Timer waitingOnSegmentAllocation;
   public final Timer waitingOnCommit;

   public CommitLogMetrics() {
      this.waitingOnSegmentAllocation = CassandraMetricsRegistry.Metrics.timer(factory.createMetricName("WaitingOnSegmentAllocation"));
      this.waitingOnCommit = CassandraMetricsRegistry.Metrics.timer(factory.createMetricName("WaitingOnCommit"));
   }

   public void attach(final AbstractCommitLogService service, final AbstractCommitLogSegmentManager segmentManager) {
      this.completedTasks = (Gauge)CassandraMetricsRegistry.Metrics.register(factory.createMetricName("CompletedTasks"), new Gauge<Long>() {
         public Long getValue() {
            return Long.valueOf(service.getCompletedTasks());
         }
      });
      this.pendingTasks = (Gauge)CassandraMetricsRegistry.Metrics.register(factory.createMetricName("PendingTasks"), new Gauge<Long>() {
         public Long getValue() {
            return Long.valueOf(service.getPendingTasks());
         }
      });
      this.totalCommitLogSize = (Gauge)CassandraMetricsRegistry.Metrics.register(factory.createMetricName("TotalCommitLogSize"), new Gauge<Long>() {
         public Long getValue() {
            return Long.valueOf(segmentManager.onDiskSize());
         }
      });
   }
}
