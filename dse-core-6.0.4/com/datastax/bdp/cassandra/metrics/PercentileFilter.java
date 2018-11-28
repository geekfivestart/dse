package com.datastax.bdp.cassandra.metrics;

import com.codahale.metrics.Timer;
import com.datastax.bdp.cassandra.cql3.CqlSlowLogPlugin;
import com.datastax.bdp.jmx.JMX;
import com.datastax.bdp.plugin.PerformanceObjectsController;
import com.datastax.bdp.util.MapBuilder;
import com.google.common.util.concurrent.AtomicDouble;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PercentileFilter {
   private static final Logger logger = LoggerFactory.getLogger(CqlSlowLogPlugin.class);
   public static final long DEFAULT_SAMPLE_SIZE = 100L;
   public static final double DEFAULT_PERCENTILE = 0.0D;
   public static final String MINIMUM_SAMPLES_PROPERTY_NAME = "minimumSamples";
   private static final double UNIT = 1000000.0D;
   private final Timer queriesLatency;
   private volatile double percentile;
   private volatile long minimumSamples;
   private final AtomicDouble percentileValue;

   public PercentileFilter() {
      this(0.0D, 100L);
   }

   public PercentileFilter(double percentile, long minimumSamples) {
      this.percentileValue = new AtomicDouble();
      this.percentile = percentile;
      this.minimumSamples = minimumSamples;
      this.queriesLatency = CassandraMetricsRegistry.Metrics.timer(metricName());
   }

   public static String metricName() {
      return JMX.buildMBeanName(JMX.Type.PERF_OBJECTS, MapBuilder.<String,String>immutable().withKeys(new String[]{"name", "metrics"}).withValues(new String[]{PerformanceObjectsController.getPerfBeanName(PerformanceObjectsController.CqlSlowLogBean.class), "queriesLatency"}).build());
   }

   public long getMinimumSamples() {
      return this.minimumSamples;
   }

   public final void setMinimumSamples(long minimumSamples) {
      this.minimumSamples = minimumSamples;
   }

   public double getPercentile() {
      return this.percentile;
   }

   public void setPercentile(double percentile) {
      this.percentile = percentile;
      this.computePercentileValue();
   }

   public double getPercentileValue() {
      return this.percentileValue.get();
   }

   public double computePercentileValue() {
      if(this.percentile >= 0.0D && this.percentile <= 1.0D) {
         double value = this.queriesLatency.getSnapshot().getValue(this.percentile) / 1000000.0D;
         this.percentileValue.set(value);
         return value;
      } else {
         return 0.0D;
      }
   }

   public boolean isWarmedUp() {
      return this.queriesLatency.getCount() >= this.minimumSamples;
   }

   public boolean update(long datum) {
      this.queriesLatency.update(datum, TimeUnit.MILLISECONDS);
      long count = this.queriesLatency.getCount();
      if(count == this.minimumSamples) {
         logger.info("CqlSlowLog plugin now warmed up!");
         return true;
      } else {
         return false;
      }
   }
}
