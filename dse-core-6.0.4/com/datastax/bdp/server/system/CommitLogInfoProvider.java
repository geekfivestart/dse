package com.datastax.bdp.server.system;

import com.codahale.metrics.JmxReporter.JmxGaugeMBean;
import java.lang.management.ManagementFactory;
import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

public interface CommitLogInfoProvider {
   long getCommitLogTotalSize();

   long getCommitLogPendingTasks();

   public static class JmxCommitLogInfoProvider implements CommitLogInfoProvider {
      JmxGaugeMBean size;
      JmxGaugeMBean pending;

      public JmxCommitLogInfoProvider() {
         MBeanServer server = ManagementFactory.getPlatformMBeanServer();

         try {
            this.size = (JmxGaugeMBean)JMX.newMBeanProxy(server, ObjectName.getInstance("org.apache.cassandra.metrics:type=CommitLog,name=TotalCommitLogSize"), JmxGaugeMBean.class);
            this.pending = (JmxGaugeMBean)JMX.newMBeanProxy(server, ObjectName.getInstance("org.apache.cassandra.metrics:type=CommitLog,name=PendingTasks"), JmxGaugeMBean.class);
         } catch (MalformedObjectNameException var3) {
            throw new RuntimeException(var3);
         }
      }

      public long getCommitLogTotalSize() {
         return ((Long)this.size.getValue()).longValue();
      }

      public long getCommitLogPendingTasks() {
         return ((Long)this.pending.getValue()).longValue();
      }
   }
}
