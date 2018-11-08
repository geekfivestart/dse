package org.apache.cassandra.metrics;

import java.net.InetAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessagingMetrics {
   private static Logger logger = LoggerFactory.getLogger(MessagingMetrics.class);
   private static final MetricNameFactory factory = new DefaultNameFactory("Messaging");
   public final Timer crossNodeLatency;
   public final ConcurrentHashMap<String, Timer> dcLatency;
   public final ConcurrentHashMap<String, Timer> queueWaitLatency;

   public MessagingMetrics() {
      this.crossNodeLatency = CassandraMetricsRegistry.Metrics.timer(factory.createMetricName("CrossNodeLatency"));
      this.dcLatency = new ConcurrentHashMap();
      this.queueWaitLatency = new ConcurrentHashMap();
   }

   public void addTimeTaken(InetAddress from, long timeTaken) {
      String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(from);
      Timer timer = (Timer)this.dcLatency.get(dc);
      if(timer == null) {
         timer = (Timer)this.dcLatency.computeIfAbsent(dc, (k) -> {
            return CassandraMetricsRegistry.Metrics.timer(factory.createMetricName(dc + "-Latency"));
         });
      }

      timer.update(timeTaken, TimeUnit.MILLISECONDS);
      this.crossNodeLatency.update(timeTaken, TimeUnit.MILLISECONDS);
   }

   public void addQueueWaitTime(String messageType, long timeTaken) {
      if(timeTaken >= 0L) {
         Timer timer = (Timer)this.queueWaitLatency.get(messageType);
         if(timer == null) {
            timer = (Timer)this.queueWaitLatency.computeIfAbsent(messageType, (k) -> {
               return CassandraMetricsRegistry.Metrics.timer(factory.createMetricName(messageType + "-WaitLatency"));
            });
         }

         timer.update(timeTaken, TimeUnit.MILLISECONDS);
      }
   }
}
