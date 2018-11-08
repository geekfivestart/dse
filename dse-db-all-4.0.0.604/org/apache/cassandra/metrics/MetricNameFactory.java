package org.apache.cassandra.metrics;

public interface MetricNameFactory {
   CassandraMetricsRegistry.MetricName createMetricName(String var1);
}
