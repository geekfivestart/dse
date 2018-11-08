package com.datastax.bdp.server.system;

import com.codahale.metrics.JmxReporter.JmxTimerMBean;
import java.lang.management.ManagementFactory;
import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

public interface ClientRequestMetricsProvider {
   long totalReads();

   long totalRangeSlices();

   long totalWrites();

   double meanReadLatency();

   double meanRangeSliceLatency();

   double meanWriteLatency();

   long readTimeouts();

   long rangeSliceTimeouts();

   long writeTimeouts();

   public static class JmxClientRequestMetricsProvider implements ClientRequestMetricsProvider {
      JmxTimerMBean readLatency;
      JmxTimerMBean rangeLatency;
      JmxTimerMBean writeLatency;
      JmxTimerMBean readTimeouts;
      JmxTimerMBean rangeTimeouts;
      JmxTimerMBean writeTimeouts;

      public JmxClientRequestMetricsProvider() {
         MBeanServer server = ManagementFactory.getPlatformMBeanServer();

         try {
            this.readLatency = (JmxTimerMBean)JMX.newMBeanProxy(server, ObjectName.getInstance("org.apache.cassandra.metrics:type=ClientRequest,scope=Read,name=Latency"), JmxTimerMBean.class);
            this.rangeLatency = (JmxTimerMBean)JMX.newMXBeanProxy(server, ObjectName.getInstance("org.apache.cassandra.metrics:type=ClientRequest,scope=RangeSlice,name=Latency"), JmxTimerMBean.class);
            this.writeLatency = (JmxTimerMBean)JMX.newMXBeanProxy(server, ObjectName.getInstance("org.apache.cassandra.metrics:type=ClientRequest,scope=Write,name=Latency"), JmxTimerMBean.class);
            this.readTimeouts = (JmxTimerMBean)JMX.newMBeanProxy(server, ObjectName.getInstance("org.apache.cassandra.metrics:type=ClientRequest,scope=Read,name=Timeouts"), JmxTimerMBean.class);
            this.rangeTimeouts = (JmxTimerMBean)JMX.newMBeanProxy(server, ObjectName.getInstance("org.apache.cassandra.metrics:type=ClientRequest,scope=RangeSlice,name=Timeouts"), JmxTimerMBean.class);
            this.writeTimeouts = (JmxTimerMBean)JMX.newMBeanProxy(server, ObjectName.getInstance("org.apache.cassandra.metrics:type=ClientRequest,scope=Write,name=Timeouts"), JmxTimerMBean.class);
         } catch (MalformedObjectNameException var3) {
            throw new RuntimeException(var3);
         }
      }

      public long totalReads() {
         return this.readLatency.getCount();
      }

      public long totalRangeSlices() {
         return this.rangeLatency.getCount();
      }

      public long totalWrites() {
         return this.writeLatency.getCount();
      }

      public double meanReadLatency() {
         return this.readLatency.getMean();
      }

      public double meanRangeSliceLatency() {
         return this.rangeLatency.getMean();
      }

      public double meanWriteLatency() {
         return this.writeLatency.getMean();
      }

      public long readTimeouts() {
         return this.readTimeouts.getCount();
      }

      public long rangeSliceTimeouts() {
         return this.rangeTimeouts.getCount();
      }

      public long writeTimeouts() {
         return this.writeTimeouts.getCount();
      }
   }
}
