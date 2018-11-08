package org.apache.cassandra.metrics;

import java.util.concurrent.TimeUnit;

public class LatencyMetrics {
   public final Timer latency;
   public final Counter totalLatency;
   protected final MetricNameFactory factory;
   protected final MetricNameFactory aliasFactory;
   protected final String namePrefix;
   private final boolean isComposite;

   public LatencyMetrics(String type, String scope) {
      this(type, scope, false);
   }

   public LatencyMetrics(String type, String scope, boolean isComposite) {
      this(type, "", scope, isComposite);
   }

   public LatencyMetrics(String type, String namePrefix, String scope, boolean isComposite) {
      this((MetricNameFactory)(new DefaultNameFactory(type, scope)), namePrefix, isComposite);
   }

   public LatencyMetrics(MetricNameFactory factory, String namePrefix, boolean isComposite) {
      this((MetricNameFactory)factory, (MetricNameFactory)null, namePrefix, isComposite);
   }

   public LatencyMetrics(MetricNameFactory factory, MetricNameFactory aliasFactory, String namePrefix, boolean isComposite) {
      this.factory = factory;
      this.aliasFactory = aliasFactory;
      this.namePrefix = namePrefix;
      this.isComposite = isComposite;
      if(aliasFactory == null) {
         this.latency = CassandraMetricsRegistry.Metrics.timer(factory.createMetricName(namePrefix + "Latency"), isComposite);
         this.totalLatency = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName(namePrefix + "TotalLatency"), isComposite);
      } else {
         this.latency = CassandraMetricsRegistry.Metrics.timer(factory.createMetricName(namePrefix + "Latency"), aliasFactory.createMetricName(namePrefix + "Latency"), isComposite);
         this.totalLatency = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName(namePrefix + "TotalLatency"), aliasFactory.createMetricName(namePrefix + "TotalLatency"), isComposite);
      }

   }

   public LatencyMetrics(MetricNameFactory factory, MetricNameFactory aliasFactory, String namePrefix, LatencyMetrics... parents) {
      this(factory, aliasFactory, namePrefix, false);
      LatencyMetrics[] var5 = parents;
      int var6 = parents.length;

      for(int var7 = 0; var7 < var6; ++var7) {
         LatencyMetrics parent = var5[var7];
         parent.compose(this);
      }

   }

   private void compose(LatencyMetrics child) {
      if(!this.isComposite) {
         throw new UnsupportedOperationException("Non composite latency metrics cannot be composed with another metric");
      } else {
         this.latency.compose(child.latency);
         this.totalLatency.compose(child.totalLatency);
      }
   }

   public void addNano(long nanos) {
      this.latency.update(nanos, TimeUnit.NANOSECONDS);
      this.totalLatency.inc(nanos / 1000L);
   }

   public void release() {
      if(this.aliasFactory == null) {
         CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName(this.namePrefix + "Latency"));
         CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName(this.namePrefix + "TotalLatency"));
      } else {
         CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName(this.namePrefix + "Latency"), this.aliasFactory.createMetricName(this.namePrefix + "Latency"));
         CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName(this.namePrefix + "TotalLatency"), this.aliasFactory.createMetricName(this.namePrefix + "TotalLatency"));
      }

   }
}
