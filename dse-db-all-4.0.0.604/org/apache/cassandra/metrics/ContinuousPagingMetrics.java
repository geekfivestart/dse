package org.apache.cassandra.metrics;

import com.codahale.metrics.Gauge;
import org.apache.cassandra.cql3.continuous.paging.ContinuousPagingService;

public class ContinuousPagingMetrics {
   private final MetricNameFactory factory = new DefaultNameFactory("ContinuousPaging", "");
   private final LatencyMetrics optimizedPathLatency = new LatencyMetrics("ContinuousPaging", "OptimizedPathLatency");
   private final LatencyMetrics slowPathLatency = new LatencyMetrics("ContinuousPaging", "SlowPathLatency");
   public final Gauge liveSessions = CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName("LiveSessions"), ContinuousPagingService::liveSessions);
   public final Gauge pendingPages = CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName("PendingPages"), ContinuousPagingService::pendingPages);
   public final Meter requests;
   public final Meter creationFailures;
   public final Meter tooManySessions;
   public final Meter clientWriteExceptions;
   public final Meter failures;
   public final LatencyMetrics waitingTime;
   public final Counter serverBlocked;
   public final LatencyMetrics serverBlockedLatency;

   public ContinuousPagingMetrics() {
      this.requests = CassandraMetricsRegistry.Metrics.meter(this.factory.createMetricName("Requests"));
      this.creationFailures = CassandraMetricsRegistry.Metrics.meter(this.factory.createMetricName("CreationFailures"));
      this.tooManySessions = CassandraMetricsRegistry.Metrics.meter(this.factory.createMetricName("TooManySessions"));
      this.clientWriteExceptions = CassandraMetricsRegistry.Metrics.meter(this.factory.createMetricName("ClientWriteExceptions"));
      this.failures = CassandraMetricsRegistry.Metrics.meter(this.factory.createMetricName("Failures"));
      this.waitingTime = new LatencyMetrics("ContinuousPaging", "WaitingTime");
      this.serverBlocked = CassandraMetricsRegistry.Metrics.counter(this.factory.createMetricName("ServerBlocked"));
      this.serverBlockedLatency = new LatencyMetrics("ContinuousPaging", "ServerBlockedLatency");
   }

   public void addTotalDuration(boolean isLocal, long nanos) {
      if(isLocal) {
         this.optimizedPathLatency.addNano(nanos);
      } else {
         this.slowPathLatency.addNano(nanos);
      }

   }

   public void release() {
      this.optimizedPathLatency.release();
      this.slowPathLatency.release();
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("LiveSessions"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("PendingPages"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("Requests"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("CreationFailures"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("TooManySessions"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("ClientWriteExceptions"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("Failures"));
      this.waitingTime.release();
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("ServerBlocked"));
      this.serverBlockedLatency.release();
   }
}
