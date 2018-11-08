package org.apache.cassandra.metrics;

public class DefaultNameFactory extends AbstractMetricNameFactory {
   public static final String GROUP_NAME = "org.apache.cassandra.metrics";

   public DefaultNameFactory(String type) {
      this(type, (String)null);
   }

   public DefaultNameFactory(String type, String scope) {
      super("org.apache.cassandra.metrics", type, scope);
   }

   public static CassandraMetricsRegistry.MetricName createMetricName(String type, String metricName, String scope) {
      return createMetricName("org.apache.cassandra.metrics", type, (String)null, (String)null, scope, metricName);
   }
}
