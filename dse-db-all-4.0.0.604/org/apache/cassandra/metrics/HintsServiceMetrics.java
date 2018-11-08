package org.apache.cassandra.metrics;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.util.concurrent.MoreExecutors;
import java.net.InetAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HintsServiceMetrics {
   private static final Logger logger = LoggerFactory.getLogger(HintsServiceMetrics.class);
   private static final MetricNameFactory factory = new DefaultNameFactory("HintsService");
   public static final Meter hintsSucceeded;
   public static final Meter hintsFailed;
   public static final Meter hintsTimedOut;
   private static final Histogram globalDelayHistogram;
   private static final LoadingCache<InetAddress, Histogram> delayByEndpoint;

   public HintsServiceMetrics() {
   }

   public static void updateDelayMetrics(InetAddress endpoint, long delay) {
      if(delay <= 0L) {
         logger.warn("Invalid negative latency in hint delivery delay: {}", Long.valueOf(delay));
      } else {
         globalDelayHistogram.update(delay);
         ((Histogram)delayByEndpoint.get(endpoint)).update(delay);
      }
   }

   static {
      hintsSucceeded = CassandraMetricsRegistry.Metrics.meter(factory.createMetricName("HintsSucceeded"));
      hintsFailed = CassandraMetricsRegistry.Metrics.meter(factory.createMetricName("HintsFailed"));
      hintsTimedOut = CassandraMetricsRegistry.Metrics.meter(factory.createMetricName("HintsTimedOut"));
      globalDelayHistogram = CassandraMetricsRegistry.Metrics.histogram(factory.createMetricName("Hint_delays"), false);
      delayByEndpoint = Caffeine.newBuilder().executor(MoreExecutors.directExecutor()).build((address) -> {
         return CassandraMetricsRegistry.Metrics.histogram(factory.createMetricName("Hint_delays-" + address.getHostAddress().replace(':', '.')), false);
      });
   }
}
