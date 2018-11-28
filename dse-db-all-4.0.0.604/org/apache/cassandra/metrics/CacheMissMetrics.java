package org.apache.cassandra.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.RatioGauge;
import com.codahale.metrics.RatioGauge.Ratio;
import org.apache.cassandra.cache.CacheSize;

public class CacheMissMetrics {
   public final Gauge<Long> capacity;
   public final Meter misses;
   public final Meter requests;
   public final Meter notInCacheExceptions;
   public final Timer missLatency;
   public final Gauge<Double> hitRate;
   public final Gauge<Double> oneMinuteHitRate;
   public final Gauge<Double> fiveMinuteHitRate;
   public final Gauge<Double> fifteenMinuteHitRate;
   public final Gauge<Long> size;
   public final Gauge<Integer> entries;

    public CacheMissMetrics(String type, CacheSize cache) {
        DefaultNameFactory factory = new DefaultNameFactory("Cache", type);
        this.capacity = CassandraMetricsRegistry.Metrics.register(factory.createMetricName("Capacity"), cache::capacity);
        this.misses = CassandraMetricsRegistry.Metrics.meter(factory.createMetricName("Misses"));
        this.requests = CassandraMetricsRegistry.Metrics.meter(factory.createMetricName("Requests"));
        this.notInCacheExceptions = CassandraMetricsRegistry.Metrics.meter(factory.createMetricName("NotInCacheExceptions"));
        this.missLatency = CassandraMetricsRegistry.Metrics.timer(factory.createMetricName("MissLatency"));
        this.hitRate = (Gauge)CassandraMetricsRegistry.Metrics.register(factory.createMetricName("HitRate"), new RatioGauge(){

            public RatioGauge.Ratio getRatio() {
                long req = CacheMissMetrics.this.requests.getCount();
                long mis = CacheMissMetrics.this.misses.getCount();
                return RatioGauge.Ratio.of((double)(req - mis), (double)req);
            }
        });
        this.oneMinuteHitRate = (Gauge)CassandraMetricsRegistry.Metrics.register(factory.createMetricName("OneMinuteHitRate"), new RatioGauge(){

            protected RatioGauge.Ratio getRatio() {
                double req = CacheMissMetrics.this.requests.getOneMinuteRate();
                double mis = CacheMissMetrics.this.misses.getOneMinuteRate();
                return RatioGauge.Ratio.of((double)(req - mis), (double)req);
            }
        });
        this.fiveMinuteHitRate = (Gauge)CassandraMetricsRegistry.Metrics.register(factory.createMetricName("FiveMinuteHitRate"), new RatioGauge(){

            protected RatioGauge.Ratio getRatio() {
                double req = CacheMissMetrics.this.requests.getFiveMinuteRate();
                double mis = CacheMissMetrics.this.misses.getFiveMinuteRate();
                return RatioGauge.Ratio.of((double)(req - mis), (double)req);
            }
        });
        this.fifteenMinuteHitRate = (Gauge)CassandraMetricsRegistry.Metrics.register(factory.createMetricName("FifteenMinuteHitRate"), new RatioGauge(){

            protected RatioGauge.Ratio getRatio() {
                double req = CacheMissMetrics.this.requests.getFifteenMinuteRate();
                double mis = CacheMissMetrics.this.misses.getFifteenMinuteRate();
                return RatioGauge.Ratio.of((double)(req - mis), (double)req);
            }
        });
        this.size = CassandraMetricsRegistry.Metrics.register(factory.createMetricName("Size"), cache::weightedSize);
        this.entries = CassandraMetricsRegistry.Metrics.register(factory.createMetricName("Entries"), cache::size);
    }

   public void reset() {
      this.requests.mark(-this.requests.getCount());
      this.misses.mark(-this.misses.getCount());
   }

   public String toString() {
      return String.format("Requests: %s, Misses: %s, NotInCacheExceptions: %s, missLatency: %s", new Object[]{this.requests, this.misses, this.notInCacheExceptions, this.missLatency});
   }
}
