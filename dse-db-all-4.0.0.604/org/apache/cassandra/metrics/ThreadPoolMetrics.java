package org.apache.cassandra.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.JmxReporter.JmxCounterMBean;
import com.codahale.metrics.JmxReporter.JmxGaugeMBean;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.QueryExp;

public class ThreadPoolMetrics {
   public final Gauge<Integer> activeTasks;
   public final Counter totalBlocked;
   public final Counter currentBlocked;
   public final Gauge<Long> completedTasks;
   public final Gauge<Long> pendingTasks;
   public final Gauge<Integer> maxPoolSize;
   private MetricNameFactory factory;

   public ThreadPoolMetrics(final ThreadPoolExecutor executor, String path, String poolName) {
      this.factory = new ThreadPoolMetricNameFactory("ThreadPools", path, poolName);
      this.activeTasks = (Gauge)CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName("ActiveTasks"), new Gauge<Integer>() {
         public Integer getValue() {
            return Integer.valueOf(executor.getActiveCount());
         }
      });
      this.totalBlocked = CassandraMetricsRegistry.Metrics.counter(this.factory.createMetricName("TotalBlockedTasks"));
      this.currentBlocked = CassandraMetricsRegistry.Metrics.counter(this.factory.createMetricName("CurrentlyBlockedTasks"));
      this.completedTasks = (Gauge)CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName("CompletedTasks"), new Gauge<Long>() {
         public Long getValue() {
            return Long.valueOf(executor.getCompletedTaskCount());
         }
      });
      this.pendingTasks = (Gauge)CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName("PendingTasks"), new Gauge<Long>() {
         public Long getValue() {
            return Long.valueOf(executor.getTaskCount() - executor.getCompletedTaskCount());
         }
      });
      this.maxPoolSize = (Gauge)CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName("MaxPoolSize"), new Gauge<Integer>() {
         public Integer getValue() {
            return Integer.valueOf(executor.getMaximumPoolSize());
         }
      });
   }

   public void release() {
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("ActiveTasks"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("PendingTasks"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("CompletedTasks"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("TotalBlockedTasks"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("CurrentlyBlockedTasks"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("MaxPoolSize"));
   }

   public static Object getJmxMetric(MBeanServerConnection mbeanServerConn, String jmxPath, String poolName, String metricName) {
      String name = String.format("org.apache.cassandra.metrics:type=ThreadPools,path=%s,scope=%s,name=%s", new Object[]{jmxPath, poolName, metricName});

      try {
         ObjectName oName = new ObjectName(name);
         if(!mbeanServerConn.isRegistered(oName)) {
            oName = new ObjectName(name + "Gauge");
            metricName = metricName + "Gauge";
            if(!mbeanServerConn.isRegistered(oName)) {
               return "N/A";
            }
         }

         byte var7 = -1;
         switch(metricName.hashCode()) {
         case -2033497731:
            if(metricName.equals("TotalBackpressureDelayedTasksGauge")) {
               var7 = 4;
            }
            break;
         case -865142584:
            if(metricName.equals("CurrentlyBlockedTasks")) {
               var7 = 7;
            }
            break;
         case -29430007:
            if(metricName.equals("TotalBackpressureCountedTasksGauge")) {
               var7 = 3;
            }
            break;
         case 316783703:
            if(metricName.equals("PendingTasks")) {
               var7 = 1;
            }
            break;
         case 551065635:
            if(metricName.equals("CompletedTasks")) {
               var7 = 2;
            }
            break;
         case 605825032:
            if(metricName.equals("ActiveTasks")) {
               var7 = 0;
            }
            break;
         case 968140358:
            if(metricName.equals("TotalBlockedTasks")) {
               var7 = 6;
            }
            break;
         case 1993127507:
            if(metricName.equals("TotalBlockedTasksGauge")) {
               var7 = 5;
            }
         }

         switch(var7) {
         case 0:
         case 1:
         case 2:
         case 3:
         case 4:
         case 5:
            return ((JmxGaugeMBean)JMX.newMBeanProxy(mbeanServerConn, oName, JmxGaugeMBean.class)).getValue();
         case 6:
         case 7:
            return Long.valueOf(((JmxCounterMBean)JMX.newMBeanProxy(mbeanServerConn, oName, JmxCounterMBean.class)).getCount());
         default:
            throw new AssertionError("Unknown metric name " + metricName);
         }
      } catch (Exception var8) {
         throw new RuntimeException("Error reading: " + name, var8);
      }
   }

   public static Multimap<String, String> getJmxThreadPools(MBeanServerConnection mbeanServerConn) {
      try {
         Multimap<String, String> threadPools = HashMultimap.create();
         Set<ObjectName> threadPoolObjectNames = mbeanServerConn.queryNames(new ObjectName("org.apache.cassandra.metrics:type=ThreadPools,*"), (QueryExp)null);
         Iterator var3 = threadPoolObjectNames.iterator();

         while(var3.hasNext()) {
            ObjectName oName = (ObjectName)var3.next();
            threadPools.put(oName.getKeyProperty("path"), oName.getKeyProperty("scope"));
         }

         return threadPools;
      } catch (MalformedObjectNameException var5) {
         throw new RuntimeException("Bad query to JMX server: ", var5);
      } catch (IOException var6) {
         throw new RuntimeException("Error getting threadpool names from JMX", var6);
      }
   }
}
