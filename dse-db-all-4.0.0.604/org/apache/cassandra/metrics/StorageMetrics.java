package org.apache.cassandra.metrics;

public class StorageMetrics {
   private static final MetricNameFactory factory = new DefaultNameFactory("Storage");
   public static final com.codahale.metrics.Counter load;
   public static final com.codahale.metrics.Counter uncaughtExceptions;
   public static final com.codahale.metrics.Counter totalHintsInProgress;
   public static final com.codahale.metrics.Counter totalHintsReplayed;
   public static final com.codahale.metrics.Counter repairExceptions;
   public static final com.codahale.metrics.Meter batchlogReplays;
   public static final com.codahale.metrics.Meter hintedBatchlogReplays;
   public static final com.codahale.metrics.Counter totalHints;
   public static final com.codahale.metrics.Counter hintsOnDisk;

   public StorageMetrics() {
   }

   static {
      load = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("Load"));
      uncaughtExceptions = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("Exceptions"));
      totalHintsInProgress = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("TotalHintsInProgress"));
      totalHintsReplayed = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("TotalHintsReplayed"));
      repairExceptions = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("RepairExceptions"));
      batchlogReplays = CassandraMetricsRegistry.Metrics.meter(factory.createMetricName("BatchlogReplays"));
      hintedBatchlogReplays = CassandraMetricsRegistry.Metrics.meter(factory.createMetricName("HintedBatchlogReplays"));
      totalHints = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("TotalHints"));
      hintsOnDisk = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("HintsOnDisk"));
   }
}
