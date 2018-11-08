package org.apache.cassandra.metrics;

public class ClientRequestMetrics extends LatencyMetrics {
   public final Meter timeouts;
   public final Meter unavailables;
   public final Meter failures;

   public ClientRequestMetrics(String scope) {
      super("ClientRequest", scope);
      this.timeouts = CassandraMetricsRegistry.Metrics.meter(this.factory.createMetricName("Timeouts"));
      this.unavailables = CassandraMetricsRegistry.Metrics.meter(this.factory.createMetricName("Unavailables"));
      this.failures = CassandraMetricsRegistry.Metrics.meter(this.factory.createMetricName("Failures"));
   }

   public void release() {
      super.release();
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("Timeouts"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("Unavailables"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("Failures"));
   }
}
