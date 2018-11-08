package org.apache.cassandra.metrics;

import com.codahale.metrics.Gauge;
import java.net.InetAddress;
import org.apache.cassandra.net.OutboundTcpConnectionPool;

public class ConnectionMetrics {
   public static final String TYPE_NAME = "Connection";
   public static final Meter totalTimeouts;
   public final String address;
   public final Gauge<Integer> largeMessagePendingTasks;
   public final Gauge<Long> largeMessageCompletedTasks;
   public final Gauge<Long> largeMessageDroppedTasks;
   public final Gauge<Integer> smallMessagePendingTasks;
   public final Gauge<Long> smallMessageCompletedTasks;
   public final Gauge<Long> smallMessageDroppedTasks;
   public final Gauge<Integer> gossipMessagePendingTasks;
   public final Gauge<Long> gossipMessageCompletedTasks;
   public final Gauge<Long> gossipMessageDroppedTasks;
   public final Meter timeouts;
   private final MetricNameFactory factory;

   public ConnectionMetrics(InetAddress ip, final OutboundTcpConnectionPool connectionPool) {
      this.address = ip.getHostAddress().replace(':', '.');
      this.factory = new DefaultNameFactory("Connection", this.address);
      this.largeMessagePendingTasks = (Gauge)CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName("LargeMessagePendingTasks"), new Gauge<Integer>() {
         public Integer getValue() {
            return Integer.valueOf(connectionPool.large().getPendingMessages());
         }
      });
      this.largeMessageCompletedTasks = (Gauge)CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName("LargeMessageCompletedTasks"), new Gauge<Long>() {
         public Long getValue() {
            return Long.valueOf(connectionPool.large().getCompletedMesssages());
         }
      });
      this.largeMessageDroppedTasks = (Gauge)CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName("LargeMessageDroppedTasks"), new Gauge<Long>() {
         public Long getValue() {
            return Long.valueOf(connectionPool.large().getDroppedMessages());
         }
      });
      this.smallMessagePendingTasks = (Gauge)CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName("SmallMessagePendingTasks"), new Gauge<Integer>() {
         public Integer getValue() {
            return Integer.valueOf(connectionPool.small().getPendingMessages());
         }
      });
      this.smallMessageCompletedTasks = (Gauge)CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName("SmallMessageCompletedTasks"), new Gauge<Long>() {
         public Long getValue() {
            return Long.valueOf(connectionPool.small().getCompletedMesssages());
         }
      });
      this.smallMessageDroppedTasks = (Gauge)CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName("SmallMessageDroppedTasks"), new Gauge<Long>() {
         public Long getValue() {
            return Long.valueOf(connectionPool.small().getDroppedMessages());
         }
      });
      this.gossipMessagePendingTasks = (Gauge)CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName("GossipMessagePendingTasks"), new Gauge<Integer>() {
         public Integer getValue() {
            return Integer.valueOf(connectionPool.gossip().getPendingMessages());
         }
      });
      this.gossipMessageCompletedTasks = (Gauge)CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName("GossipMessageCompletedTasks"), new Gauge<Long>() {
         public Long getValue() {
            return Long.valueOf(connectionPool.gossip().getCompletedMesssages());
         }
      });
      this.gossipMessageDroppedTasks = (Gauge)CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName("GossipMessageDroppedTasks"), new Gauge<Long>() {
         public Long getValue() {
            return Long.valueOf(connectionPool.gossip().getDroppedMessages());
         }
      });
      this.timeouts = CassandraMetricsRegistry.Metrics.meter(this.factory.createMetricName("Timeouts"));
   }

   public void release() {
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("LargeMessagePendingTasks"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("LargeMessageCompletedTasks"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("LargeMessageDroppedTasks"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("SmallMessagePendingTasks"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("SmallMessageCompletedTasks"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("SmallMessageDroppedTasks"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("GossipMessagePendingTasks"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("GossipMessageCompletedTasks"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("GossipMessageDroppedTasks"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("Timeouts"));
   }

   static {
      totalTimeouts = CassandraMetricsRegistry.Metrics.meter(DefaultNameFactory.createMetricName("Connection", "TotalTimeouts", (String)null));
   }
}
