package org.apache.cassandra.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metered;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

public class CassandraMetricsRegistry extends MetricRegistry {
   public static final CassandraMetricsRegistry Metrics = new CassandraMetricsRegistry();
   private final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

   private CassandraMetricsRegistry() {
   }

   public Counter counter(CassandraMetricsRegistry.MetricName name, boolean isComposite) {
      return (Counter)this.register(name, Counter.make(isComposite));
   }

   public Counter counter(CassandraMetricsRegistry.MetricName name) {
      return this.counter(name, false);
   }

   public Counter counter(CassandraMetricsRegistry.MetricName name, CassandraMetricsRegistry.MetricName alias, boolean isComposite) {
      Counter counter = this.counter(name, isComposite);
      this.registerAlias(name, alias);
      return counter;
   }

   public Meter meter(CassandraMetricsRegistry.MetricName name) {
      return (Meter)this.register(name, new Meter());
   }

   public Meter meter(CassandraMetricsRegistry.MetricName name, CassandraMetricsRegistry.MetricName alias) {
      Meter meter = this.meter(name);
      this.registerAlias(name, alias);
      return meter;
   }

   public Histogram histogram(CassandraMetricsRegistry.MetricName name, boolean considerZeroes) {
      return this.histogram(name, considerZeroes, false);
   }

   public Histogram histogram(CassandraMetricsRegistry.MetricName name, boolean considerZeroes, boolean isComposite) {
      return (Histogram)this.register(name, Histogram.make(considerZeroes, isComposite));
   }

   public Histogram histogram(CassandraMetricsRegistry.MetricName name, CassandraMetricsRegistry.MetricName alias, boolean considerZeroes, boolean isComposite) {
      Histogram histogram = this.histogram(name, considerZeroes, isComposite);
      this.registerAlias(name, alias);
      return histogram;
   }

   public Timer timer(CassandraMetricsRegistry.MetricName name, boolean isComposite) {
      return (Timer)this.register(name, new Timer(isComposite));
   }

   public Timer timer(CassandraMetricsRegistry.MetricName name) {
      return this.timer(name, false);
   }

   public Timer timer(CassandraMetricsRegistry.MetricName name, CassandraMetricsRegistry.MetricName alias, boolean isComposite) {
      Timer timer = this.timer(name, isComposite);
      this.registerAlias(name, alias);
      return timer;
   }

   public <T extends Metric> T register(CassandraMetricsRegistry.MetricName name, T metric) {
      try {
         this.register(name.getMetricName(), metric);
         this.registerMBean(metric, name.getMBeanName());
         return metric;
      } catch (IllegalArgumentException var5) {
         Metric existing = (Metric)Metrics.getMetrics().get(name.getMetricName());
         return (T)existing;
      }
   }

   public <T extends Metric> T register(CassandraMetricsRegistry.MetricName name, CassandraMetricsRegistry.MetricName aliasName, T metric) {
      T ret = this.register(name, metric);
      this.registerAlias(name, aliasName);
      return ret;
   }

   public boolean remove(CassandraMetricsRegistry.MetricName name) {
      boolean removed = this.remove(name.getMetricName());

      try {
         this.mBeanServer.unregisterMBean(name.getMBeanName());
      } catch (Exception var4) {
         ;
      }

      return removed;
   }

   public boolean remove(CassandraMetricsRegistry.MetricName name, CassandraMetricsRegistry.MetricName alias) {
      if(this.remove(name)) {
         this.removeAlias(alias);
         return true;
      } else {
         return false;
      }
   }

   public void registerMBean(Metric metric, ObjectName name) {
      Object mbean;
      if(metric instanceof Gauge) {
         mbean = new CassandraMetricsRegistry.JmxGauge((Gauge)metric, name);
      } else if(metric instanceof Counter) {
         mbean = new CassandraMetricsRegistry.JmxCounter((Counter)metric, name);
      } else if(metric instanceof Histogram) {
         mbean = new CassandraMetricsRegistry.JmxHistogram((Histogram)metric, name);
      } else if(metric instanceof Timer) {
         mbean = new CassandraMetricsRegistry.JmxTimer((Timer)metric, name, TimeUnit.SECONDS, TimeUnit.MICROSECONDS);
      } else {
         if(!(metric instanceof Metered)) {
            throw new IllegalArgumentException("Unknown metric type: " + metric.getClass());
         }

         mbean = new CassandraMetricsRegistry.JmxMeter((Metered)metric, name, TimeUnit.SECONDS);
      }

      try {
         this.mBeanServer.registerMBean(mbean, name);
      } catch (Exception var5) {
         ;
      }

   }

   private void registerAlias(CassandraMetricsRegistry.MetricName existingName, CassandraMetricsRegistry.MetricName aliasName) {
      Metric existing = (Metric)Metrics.getMetrics().get(existingName.getMetricName());

      assert existing != null : existingName + " not registered";

      this.registerMBean(existing, aliasName.getMBeanName());
   }

   private void removeAlias(CassandraMetricsRegistry.MetricName name) {
      try {
         this.mBeanServer.unregisterMBean(name.getMBeanName());
      } catch (Exception var3) {
         ;
      }

   }

   private static String withoutFinalDollar(String s) {
      int l = s.length();
      return l != 0 && 36 == s.charAt(l - 1)?s.substring(0, l - 1):s;
   }

   @VisibleForTesting
   static long[] delta(long[] now, long[] last) {
      long[] delta = new long[now.length];
      if(last == null) {
         last = new long[now.length];
      }

      for(int i = 0; i < now.length; ++i) {
         delta[i] = now[i] - (i < last.length?last[i]:0L);
      }

      return delta;
   }

   public static class MetricName implements Comparable<CassandraMetricsRegistry.MetricName> {
      private final String group;
      private final String type;
      private final String name;
      private final String scope;
      private final String mBeanName;

      public MetricName(Class<?> klass, String name) {
         this((Class)klass, name, (String)null);
      }

      public MetricName(String group, String type, String name) {
         this(group, type, name, (String)null);
      }

      public MetricName(Class<?> klass, String name, String scope) {
         this(klass.getPackage() == null?"":klass.getPackage().getName(), CassandraMetricsRegistry.withoutFinalDollar(klass.getSimpleName()), name, scope);
      }

      public MetricName(String group, String type, String name, String scope) {
         this(group, type, name, scope, createMBeanName(group, type, name, scope));
      }

      public MetricName(String group, String type, String name, String scope, String mBeanName) {
         if(group != null && type != null) {
            if(name == null) {
               throw new IllegalArgumentException("Name needs to be specified");
            } else {
               this.group = group;
               this.type = type;
               this.name = name;
               this.scope = scope;
               this.mBeanName = mBeanName;
            }
         } else {
            throw new IllegalArgumentException("Both group and type need to be specified");
         }
      }

      public String getGroup() {
         return this.group;
      }

      public String getType() {
         return this.type;
      }

      public String getName() {
         return this.name;
      }

      public String getMetricName() {
         return MetricRegistry.name(this.group, new String[]{this.type, this.name, this.scope});
      }

      public String getScope() {
         return this.scope;
      }

      public boolean hasScope() {
         return this.scope != null;
      }

      public ObjectName getMBeanName() {
         String mname = this.mBeanName;
         if(mname == null) {
            mname = this.getMetricName();
         }

         try {
            return new ObjectName(mname);
         } catch (MalformedObjectNameException var5) {
            try {
               return new ObjectName(ObjectName.quote(mname));
            } catch (MalformedObjectNameException var4) {
               throw new RuntimeException(var4);
            }
         }
      }

      public boolean equals(Object o) {
         if(this == o) {
            return true;
         } else if(o != null && this.getClass() == o.getClass()) {
            CassandraMetricsRegistry.MetricName that = (CassandraMetricsRegistry.MetricName)o;
            return this.mBeanName.equals(that.mBeanName);
         } else {
            return false;
         }
      }

      public int hashCode() {
         return this.mBeanName.hashCode();
      }

      public String toString() {
         return this.mBeanName;
      }

      public int compareTo(CassandraMetricsRegistry.MetricName o) {
         return this.mBeanName.compareTo(o.mBeanName);
      }

      private static String createMBeanName(String group, String type, String name, String scope) {
         StringBuilder nameBuilder = new StringBuilder();
         nameBuilder.append(ObjectName.quote(group));
         nameBuilder.append(":type=");
         nameBuilder.append(ObjectName.quote(type));
         if(scope != null) {
            nameBuilder.append(",scope=");
            nameBuilder.append(ObjectName.quote(scope));
         }

         if(name.length() > 0) {
            nameBuilder.append(",name=");
            nameBuilder.append(ObjectName.quote(name));
         }

         return nameBuilder.toString();
      }

      public static String chooseGroup(String group, Class<?> klass) {
         if(group == null || group.isEmpty()) {
            group = klass.getPackage() == null?"":klass.getPackage().getName();
         }

         return group;
      }

      public static String chooseType(String type, Class<?> klass) {
         if(type == null || type.isEmpty()) {
            type = CassandraMetricsRegistry.withoutFinalDollar(klass.getSimpleName());
         }

         return type;
      }

      public static String chooseName(String name, Method method) {
         if(name == null || name.isEmpty()) {
            name = method.getName();
         }

         return name;
      }
   }

   static class JmxTimer extends CassandraMetricsRegistry.JmxMeter implements CassandraMetricsRegistry.JmxTimerMBean {
      private final Timer metric;
      private final double durationFactor;
      private final String durationUnit;
      private long[] last;

      private JmxTimer(Timer metric, ObjectName objectName, TimeUnit rateUnit, TimeUnit durationUnit) {
         super(metric, objectName, rateUnit);
         this.last = null;
         this.metric = metric;
         this.durationFactor = 1.0D / (double)durationUnit.toNanos(1L);
         this.durationUnit = durationUnit.toString().toLowerCase(Locale.US);
      }

      public double get50thPercentile() {
         return this.metric.getSnapshot().getMedian() * this.durationFactor;
      }

      public double getMin() {
         return (double)this.metric.getSnapshot().getMin() * this.durationFactor;
      }

      public double getMax() {
         return (double)this.metric.getSnapshot().getMax() * this.durationFactor;
      }

      public double getMean() {
         return this.metric.getSnapshot().getMean() * this.durationFactor;
      }

      public double getStdDev() {
         return this.metric.getSnapshot().getStdDev() * this.durationFactor;
      }

      public double get75thPercentile() {
         return this.metric.getSnapshot().get75thPercentile() * this.durationFactor;
      }

      public double get95thPercentile() {
         return this.metric.getSnapshot().get95thPercentile() * this.durationFactor;
      }

      public double get98thPercentile() {
         return this.metric.getSnapshot().get98thPercentile() * this.durationFactor;
      }

      public double get99thPercentile() {
         return this.metric.getSnapshot().get99thPercentile() * this.durationFactor;
      }

      public double get999thPercentile() {
         return this.metric.getSnapshot().get999thPercentile() * this.durationFactor;
      }

      public long[] values() {
         return this.metric.getSnapshot().getValues();
      }

      public long[] getRecentValues() {
         long[] now = this.metric.getSnapshot().getValues();
         long[] delta = CassandraMetricsRegistry.delta(now, this.last);
         this.last = now;
         return delta;
      }

      public String getDurationUnit() {
         return this.durationUnit;
      }
   }

   public interface JmxTimerMBean extends CassandraMetricsRegistry.JmxMeterMBean {
      double getMin();

      double getMax();

      double getMean();

      double getStdDev();

      double get50thPercentile();

      double get75thPercentile();

      double get95thPercentile();

      double get98thPercentile();

      double get99thPercentile();

      double get999thPercentile();

      long[] values();

      long[] getRecentValues();

      String getDurationUnit();
   }

   private static class JmxMeter extends CassandraMetricsRegistry.AbstractBean implements CassandraMetricsRegistry.JmxMeterMBean {
      private final Metered metric;
      private final double rateFactor;
      private final String rateUnit;

      private JmxMeter(Metered metric, ObjectName objectName, TimeUnit rateUnit) {
         super(objectName);
         this.metric = metric;
         this.rateFactor = (double)rateUnit.toSeconds(1L);
         this.rateUnit = "events/" + this.calculateRateUnit(rateUnit);
      }

      public long getCount() {
         return this.metric.getCount();
      }

      public double getMeanRate() {
         return this.metric.getMeanRate() * this.rateFactor;
      }

      public double getOneMinuteRate() {
         return this.metric.getOneMinuteRate() * this.rateFactor;
      }

      public double getFiveMinuteRate() {
         return this.metric.getFiveMinuteRate() * this.rateFactor;
      }

      public double getFifteenMinuteRate() {
         return this.metric.getFifteenMinuteRate() * this.rateFactor;
      }

      public String getRateUnit() {
         return this.rateUnit;
      }

      private String calculateRateUnit(TimeUnit unit) {
         String s = unit.toString().toLowerCase(Locale.US);
         return s.substring(0, s.length() - 1);
      }
   }

   public interface JmxMeterMBean extends CassandraMetricsRegistry.MetricMBean {
      long getCount();

      double getMeanRate();

      double getOneMinuteRate();

      double getFiveMinuteRate();

      double getFifteenMinuteRate();

      String getRateUnit();
   }

   private static class JmxCounter extends CassandraMetricsRegistry.AbstractBean implements CassandraMetricsRegistry.JmxCounterMBean {
      private final Counter metric;

      private JmxCounter(Counter metric, ObjectName objectName) {
         super(objectName);
         this.metric = metric;
      }

      public long getCount() {
         return this.metric.getCount();
      }
   }

   public interface JmxCounterMBean extends CassandraMetricsRegistry.MetricMBean {
      long getCount();
   }

   private static class JmxHistogram extends CassandraMetricsRegistry.AbstractBean implements CassandraMetricsRegistry.JmxHistogramMBean {
      private final Histogram metric;
      private long[] last;

      private JmxHistogram(Histogram metric, ObjectName objectName) {
         super(objectName);
         this.last = null;
         this.metric = metric;
      }

      public double get50thPercentile() {
         return this.metric.getSnapshot().getMedian();
      }

      public long getCount() {
         return this.metric.getCount();
      }

      public long getMin() {
         return this.metric.getSnapshot().getMin();
      }

      public long getMax() {
         return this.metric.getSnapshot().getMax();
      }

      public double getMean() {
         return this.metric.getSnapshot().getMean();
      }

      public double getStdDev() {
         return this.metric.getSnapshot().getStdDev();
      }

      public double get75thPercentile() {
         return this.metric.getSnapshot().get75thPercentile();
      }

      public double get95thPercentile() {
         return this.metric.getSnapshot().get95thPercentile();
      }

      public double get98thPercentile() {
         return this.metric.getSnapshot().get98thPercentile();
      }

      public double get99thPercentile() {
         return this.metric.getSnapshot().get99thPercentile();
      }

      public double get999thPercentile() {
         return this.metric.getSnapshot().get999thPercentile();
      }

      public long[] values() {
         return this.metric.getSnapshot().getValues();
      }

      public long[] getRecentValues() {
         long[] now = this.metric.getSnapshot().getValues();
         long[] delta = CassandraMetricsRegistry.delta(now, this.last);
         this.last = now;
         return delta;
      }
   }

   public interface JmxHistogramMBean extends CassandraMetricsRegistry.MetricMBean {
      long getCount();

      long getMin();

      long getMax();

      double getMean();

      double getStdDev();

      double get50thPercentile();

      double get75thPercentile();

      double get95thPercentile();

      double get98thPercentile();

      double get99thPercentile();

      double get999thPercentile();

      long[] values();

      long[] getRecentValues();
   }

   private static class JmxGauge extends CassandraMetricsRegistry.AbstractBean implements CassandraMetricsRegistry.JmxGaugeMBean {
      private final Gauge<?> metric;

      private JmxGauge(Gauge<?> metric, ObjectName objectName) {
         super(objectName);
         this.metric = metric;
      }

      public Object getValue() {
         return this.metric.getValue();
      }
   }

   public interface JmxGaugeMBean extends CassandraMetricsRegistry.MetricMBean {
      Object getValue();
   }

   private abstract static class AbstractBean implements CassandraMetricsRegistry.MetricMBean {
      private final ObjectName objectName;

      AbstractBean(ObjectName objectName) {
         this.objectName = objectName;
      }

      public ObjectName objectName() {
         return this.objectName;
      }
   }

   public interface MetricMBean {
      ObjectName objectName();
   }
}
