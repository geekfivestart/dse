package org.apache.cassandra.metrics;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.cassandra.net.DroppedMessages;

public class DroppedMessageMetrics {
   public final Meter dropped;
   public final Timer internalDroppedLatency;
   public final Timer crossNodeDroppedLatency;
   private final AtomicInteger internalDropped;
   private final AtomicInteger crossNodeDropped;

   public DroppedMessageMetrics(DroppedMessages.Group group) {
      MetricNameFactory factory = new DefaultNameFactory("DroppedMessage", group.toString());
      this.dropped = CassandraMetricsRegistry.Metrics.meter(factory.createMetricName("Dropped"));
      this.internalDroppedLatency = CassandraMetricsRegistry.Metrics.timer(factory.createMetricName("InternalDroppedLatency"));
      this.crossNodeDroppedLatency = CassandraMetricsRegistry.Metrics.timer(factory.createMetricName("CrossNodeDroppedLatency"));
      this.internalDropped = new AtomicInteger();
      this.crossNodeDropped = new AtomicInteger();
   }

   public void onMessageDropped(long timeTakenMillis, boolean hasCrossedNode) {
      this.dropped.mark();
      if(hasCrossedNode) {
         this.crossNodeDropped.incrementAndGet();
         this.crossNodeDroppedLatency.update(timeTakenMillis, TimeUnit.MILLISECONDS);
      } else {
         this.internalDropped.incrementAndGet();
         this.internalDroppedLatency.update(timeTakenMillis, TimeUnit.MILLISECONDS);
      }

   }

   public int getAndResetInternalDropped() {
      return this.internalDropped.getAndSet(0);
   }

   public int getAndResetCrossNodeDropped() {
      return this.crossNodeDropped.getAndSet(0);
   }
}
