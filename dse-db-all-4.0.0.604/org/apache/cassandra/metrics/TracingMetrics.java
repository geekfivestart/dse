package org.apache.cassandra.metrics;

public class TracingMetrics {
   public final Meter droppedTasks;

   public TracingMetrics() {
      this(new DefaultNameFactory("Tracing"));
   }

   private TracingMetrics(MetricNameFactory factory) {
      this.droppedTasks = CassandraMetricsRegistry.Metrics.meter(factory.createMetricName("DroppedTasks"));
   }
}
