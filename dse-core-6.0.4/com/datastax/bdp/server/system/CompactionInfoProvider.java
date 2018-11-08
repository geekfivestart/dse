package com.datastax.bdp.server.system;

import com.codahale.metrics.JmxReporter.JmxGaugeMBean;
import com.codahale.metrics.JmxReporter.JmxMeterMBean;
import java.lang.management.ManagementFactory;
import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

public interface CompactionInfoProvider {
   long totalCompleted();

   int pendingTasks();

   public static class JmxCompactionInfoProvider implements CompactionInfoProvider {
      JmxMeterMBean completedCompactions;
      JmxGaugeMBean pendingTasks;

      public JmxCompactionInfoProvider() {
         MBeanServer server = ManagementFactory.getPlatformMBeanServer();

         try {
            this.completedCompactions = (JmxMeterMBean)JMX.newMBeanProxy(server, ObjectName.getInstance("org.apache.cassandra.metrics:type=Compaction,name=TotalCompactionsCompleted"), JmxMeterMBean.class);
            this.pendingTasks = (JmxGaugeMBean)JMX.newMBeanProxy(server, ObjectName.getInstance("org.apache.cassandra.metrics:type=Compaction,name=PendingTasks"), JmxGaugeMBean.class);
         } catch (MalformedObjectNameException var3) {
            throw new RuntimeException(var3);
         }
      }

      public long totalCompleted() {
         return this.completedCompactions.getCount();
      }

      public int pendingTasks() {
         return ((Integer)this.pendingTasks.getValue()).intValue();
      }
   }
}
