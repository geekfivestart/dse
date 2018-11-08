package org.apache.cassandra.metrics;

public class BatchMetrics {
   private static final MetricNameFactory factory = new DefaultNameFactory("Batch");
   public final Histogram partitionsPerLoggedBatch;
   public final Histogram partitionsPerUnloggedBatch;
   public final Histogram partitionsPerCounterBatch;

   public BatchMetrics() {
      this.partitionsPerLoggedBatch = CassandraMetricsRegistry.Metrics.histogram(factory.createMetricName("PartitionsPerLoggedBatch"), false);
      this.partitionsPerUnloggedBatch = CassandraMetricsRegistry.Metrics.histogram(factory.createMetricName("PartitionsPerUnloggedBatch"), false);
      this.partitionsPerCounterBatch = CassandraMetricsRegistry.Metrics.histogram(factory.createMetricName("PartitionsPerCounterBatch"), false);
   }
}
