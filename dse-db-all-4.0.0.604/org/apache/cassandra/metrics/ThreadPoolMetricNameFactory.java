package org.apache.cassandra.metrics;

class ThreadPoolMetricNameFactory extends AbstractMetricNameFactory {
   ThreadPoolMetricNameFactory(String type, String path, String poolName) {
      super("org.apache.cassandra.metrics", type, (String)null, path, poolName);
   }
}
