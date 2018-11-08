package org.apache.cassandra.metrics;

import java.net.InetAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.Verbs;

public final class ReadCoordinationMetrics {
   private static final MetricNameFactory factory = new DefaultNameFactory("ReadCoordination");
   public static final com.codahale.metrics.Counter nonreplicaRequests;
   public static final com.codahale.metrics.Counter preferredOtherReplicas;
   private static final ConcurrentMap<InetAddress, com.codahale.metrics.Histogram> replicaLatencies;

   public ReadCoordinationMetrics() {
   }

   public static void updateReplicaLatency(Verb<?, ?> verb, InetAddress address, long latency) {
      if(verb.group().equals(Verbs.READS)) {
         if(latency != DatabaseDescriptor.getReadRpcTimeout()) {
            com.codahale.metrics.Histogram histogram = (com.codahale.metrics.Histogram)replicaLatencies.get(address);
            if(null == histogram) {
               histogram = (com.codahale.metrics.Histogram)replicaLatencies.computeIfAbsent(address, ReadCoordinationMetrics::createHistogram);
            }

            histogram.update(latency);
         }
      }
   }

   private static com.codahale.metrics.Histogram createHistogram(InetAddress a) {
      return CassandraMetricsRegistry.Metrics.histogram(DefaultNameFactory.createMetricName("ReadCoordination", "ReplicaLatency", a.getHostAddress().replace(':', '.')), false);
   }

   static {
      nonreplicaRequests = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("LocalNodeNonreplicaRequests"));
      preferredOtherReplicas = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("PreferredOtherReplicas"));
      replicaLatencies = new ConcurrentHashMap();
   }
}
