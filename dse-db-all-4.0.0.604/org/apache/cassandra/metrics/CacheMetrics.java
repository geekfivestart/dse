package org.apache.cassandra.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.RatioGauge;
import com.codahale.metrics.RatioGauge.Ratio;
import org.apache.cassandra.cache.ICache;

public class CacheMetrics {
   public final Gauge<Long> capacity;
   public final Meter hits;
   public final Meter requests;
   public final Gauge<Double> hitRate;
   public final Gauge<Double> oneMinuteHitRate;
   public final Gauge<Double> fiveMinuteHitRate;
   public final Gauge<Double> fifteenMinuteHitRate;
   public final Gauge<Long> size;
   public final Gauge<Integer> entries;

   public CacheMetrics(String type, final ICache<?, ?> cache) {
      MetricNameFactory factory = new DefaultNameFactory("Cache", type);
      this.capacity = (Gauge)CassandraMetricsRegistry.Metrics.register(factory.createMetricName("Capacity"), new Gauge<Long>() {
         public Long getValue() {
            return Long.valueOf(cache.capacity());
         }
      });
      this.hits = CassandraMetricsRegistry.Metrics.meter(factory.createMetricName("Hits"));
      this.requests = CassandraMetricsRegistry.Metrics.meter(factory.createMetricName("Requests"));
      this.hitRate = (Gauge)CassandraMetricsRegistry.Metrics.register(factory.createMetricName("HitRate"), new RatioGauge() {
         public Ratio getRatio() {
            return Ratio.of((double)CacheMetrics.this.hits.getCount(), (double)CacheMetrics.this.requests.getCount());
         }
      });
      this.oneMinuteHitRate = (Gauge)CassandraMetricsRegistry.Metrics.register(factory.createMetricName("OneMinuteHitRate"), new RatioGauge() {
         protected Ratio getRatio() {
            return Ratio.of(CacheMetrics.this.hits.getOneMinuteRate(), CacheMetrics.this.requests.getOneMinuteRate());
         }
      });
      this.fiveMinuteHitRate = (Gauge)CassandraMetricsRegistry.Metrics.register(factory.createMetricName("FiveMinuteHitRate"), new RatioGauge() {
         protected Ratio getRatio() {
            return Ratio.of(CacheMetrics.this.hits.getFiveMinuteRate(), CacheMetrics.this.requests.getFiveMinuteRate());
         }
      });
      this.fifteenMinuteHitRate = (Gauge)CassandraMetricsRegistry.Metrics.register(factory.createMetricName("FifteenMinuteHitRate"), new RatioGauge() {
         protected Ratio getRatio() {
            return Ratio.of(CacheMetrics.this.hits.getFifteenMinuteRate(), CacheMetrics.this.requests.getFifteenMinuteRate());
         }
      });
      this.size = (Gauge)CassandraMetricsRegistry.Metrics.register(factory.createMetricName("Size"), new Gauge<Long>() {
         public Long getValue() {
            return Long.valueOf(cache.weightedSize());
         }
      });
      this.entries = (Gauge)CassandraMetricsRegistry.Metrics.register(factory.createMetricName("Entries"), new Gauge<Integer>() {
         public Integer getValue() {
            return Integer.valueOf(cache.size());
         }
      });
   }
}
