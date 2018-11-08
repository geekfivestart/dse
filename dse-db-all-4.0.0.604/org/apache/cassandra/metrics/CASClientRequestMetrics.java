package org.apache.cassandra.metrics;

public class CASClientRequestMetrics extends ClientRequestMetrics {
   public final Histogram contention;
   public final Counter unfinishedCommit;

   public CASClientRequestMetrics(String scope) {
      super(scope);
      this.contention = CassandraMetricsRegistry.Metrics.histogram(this.factory.createMetricName("ContentionHistogram"), false);
      this.unfinishedCommit = CassandraMetricsRegistry.Metrics.counter(this.factory.createMetricName("UnfinishedCommit"));
   }

   public void release() {
      super.release();
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("ContentionHistogram"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("UnfinishedCommit"));
   }
}
