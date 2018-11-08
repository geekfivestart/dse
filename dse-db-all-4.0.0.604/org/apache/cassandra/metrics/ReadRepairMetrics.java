package org.apache.cassandra.metrics;

public class ReadRepairMetrics {
   private static final MetricNameFactory factory = new DefaultNameFactory("ReadRepair");
   public static final Meter repairedBlocking;
   public static final Meter repairedBackground;
   public static final Meter attempted;

   public ReadRepairMetrics() {
   }

   static {
      repairedBlocking = CassandraMetricsRegistry.Metrics.meter(factory.createMetricName("RepairedBlocking"));
      repairedBackground = CassandraMetricsRegistry.Metrics.meter(factory.createMetricName("RepairedBackground"));
      attempted = CassandraMetricsRegistry.Metrics.meter(factory.createMetricName("Attempted"));
   }
}
