package org.apache.cassandra.metrics;

import java.net.InetAddress;
import java.util.concurrent.ConcurrentMap;
import org.jctools.maps.NonBlockingHashMap;

public class StreamingMetrics {
   public static final String TYPE_NAME = "Streaming";
   private static final ConcurrentMap<InetAddress, StreamingMetrics> instances = new NonBlockingHashMap();
   public static final Counter activeStreamsOutbound;
   public static final Counter totalIncomingBytes;
   public static final Counter totalOutgoingBytes;
   public final Counter incomingBytes;
   public final Counter outgoingBytes;

   public static StreamingMetrics get(InetAddress ip) {
      StreamingMetrics metrics = (StreamingMetrics)instances.get(ip);
      if(metrics == null) {
         metrics = new StreamingMetrics(ip);
         instances.put(ip, metrics);
      }

      return metrics;
   }

   public StreamingMetrics(InetAddress peer) {
      MetricNameFactory factory = new DefaultNameFactory("Streaming", peer.getHostAddress().replace(':', '.'));
      this.incomingBytes = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("IncomingBytes"));
      this.outgoingBytes = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("OutgoingBytes"));
   }

   static {
      activeStreamsOutbound = CassandraMetricsRegistry.Metrics.counter(DefaultNameFactory.createMetricName("Streaming", "ActiveOutboundStreams", (String)null));
      totalIncomingBytes = CassandraMetricsRegistry.Metrics.counter(DefaultNameFactory.createMetricName("Streaming", "TotalIncomingBytes", (String)null));
      totalOutgoingBytes = CassandraMetricsRegistry.Metrics.counter(DefaultNameFactory.createMetricName("Streaming", "TotalOutgoingBytes", (String)null));
   }
}
