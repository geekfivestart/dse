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
      MetricNameFactory factory = new DefaultNameFactory("Cache", type);
      CassandraMetricsRegistry var10001 = CassandraMetricsRegistry.Metrics;
      CassandraMetricsRegistry.MetricName var10002 = factory.createMetricName("Capacity");
      cache.getClass();
      this.capacity = (Gauge)var10001.register(var10002, cache::capacity);
      this.misses = CassandraMetricsRegistry.Metrics.meter(factory.createMetricName("Misses"));
      this.requests = CassandraMetricsRegistry.Metrics.meter(factory.createMetricName("Requests"));
      this.notInCacheExceptions = CassandraMetricsRegistry.Metrics.meter(factory.createMetricName("NotInCacheExceptions"));
      this.missLatency = CassandraMetricsRegistry.Metrics.timer(factory.createMetricName("MissLatency"));
      this.hitRate = (Gauge)CassandraMetricsRegistry.Metrics.register(factory.createMetricName("HitRate"), new RatioGauge() {
         public Ratio getRatio() {
            long req = CacheMissMetrics.this.requests.getCount();
            long mis = CacheMissMetrics.this.misses.getCount();
            return Ratio.of((double)(req - mis), (double)req);
         }
      });
      this.oneMinuteHitRate = (Gauge)CassandraMetricsRegistry.Metrics.register(factory.createMetricName("OneMinuteHitRate"), new RatioGauge() {
         protected Ratio getRatio() {
            double req = CacheMissMetrics.this.requests.getOneMinuteRate();
            double mis = CacheMissMetrics.this.misses.getOneMinuteRate();
            return Ratio.of(req - mis, req);
         }
      });
      this.fiveMinuteHitRate = (Gauge)CassandraMetricsRegistry.Metrics.register(factory.createMetricName("FiveMinuteHitRate"), new RatioGauge() {
         protected Ratio getRatio() {
            double req = CacheMissMetrics.this.requests.getFiveMinuteRate();
            double mis = CacheMissMetrics.this.misses.getFiveMinuteRate();
            return Ratio.of(req - mis, req);
         }
      });
      this.fifteenMinuteHitRate = (Gauge)CassandraMetricsRegistry.Metrics.register(factory.createMetricName("FifteenMinuteHitRate"), new RatioGauge() {
         protected Ratio getRatio() {
            double req = CacheMissMetrics.this.requests.getFifteenMinuteRate();
            double mis = CacheMissMetrics.this.misses.getFifteenMinuteRate();
            return Ratio.of(req - mis, req);
         }
      });
      var10001 = CassandraMetricsRegistry.Metrics;
      var10002 = factory.createMetricName("Size");
      cache.getClass();
      this.size = (Gauge)var10001.register(var10002, cache::weightedSize);
      var10001 = CassandraMetricsRegistry.Metrics;
      var10002 = factory.createMetricName("Entries");
      cache.getClass();
      this.entries = (Gauge)var10001.register(var10002, cache::size);
   }

   public void reset() {
      this.requests.mark(-this.requests.getCount());
      this.misses.mark(-this.misses.getCount());
   }

   public String toString() {
      return String.format("Requests: %s, Misses: %s, NotInCacheExceptions: %s, missLatency: %s", new Object[]{this.requests, this.misses, this.notInCacheExceptions, this.missLatency});
   }
}
