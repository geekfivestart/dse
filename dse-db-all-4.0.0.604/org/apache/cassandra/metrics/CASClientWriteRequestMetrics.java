package org.apache.cassandra.metrics;

public class CASClientWriteRequestMetrics extends CASClientRequestMetrics {
   public final Histogram mutationSize;
   public final Counter conditionNotMet;

   public CASClientWriteRequestMetrics(String scope) {
      super(scope);
      this.mutationSize = CassandraMetricsRegistry.Metrics.histogram(this.factory.createMetricName("MutationSizeHistogram"), false);
      this.conditionNotMet = CassandraMetricsRegistry.Metrics.counter(this.factory.createMetricName("ConditionNotMet"));
   }

   public void release() {
      super.release();
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("ConditionNotMet"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("MutationSizeHistogram"));
   }
}
