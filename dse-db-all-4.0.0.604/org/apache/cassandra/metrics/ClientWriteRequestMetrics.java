package org.apache.cassandra.metrics;

public class ClientWriteRequestMetrics extends ClientRequestMetrics {
   public final Histogram mutationSize;

   public ClientWriteRequestMetrics(String scope) {
      super(scope);
      this.mutationSize = CassandraMetricsRegistry.Metrics.histogram(this.factory.createMetricName("MutationSizeHistogram"), false);
   }

   public void release() {
      super.release();
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("MutationSizeHistogram"));
   }
}
