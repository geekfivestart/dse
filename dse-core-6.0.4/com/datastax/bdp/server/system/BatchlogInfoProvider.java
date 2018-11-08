package com.datastax.bdp.server.system;

import java.lang.management.ManagementFactory;
import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.cassandra.batchlog.BatchlogManagerMBean;

public interface BatchlogInfoProvider {
   long getTotalBatchesReplayed();

   public static class JmxBatchlogInfoProvider implements BatchlogInfoProvider {
      private BatchlogManagerMBean batchlogManager;

      public JmxBatchlogInfoProvider() {
         MBeanServer server = ManagementFactory.getPlatformMBeanServer();

         try {
            this.batchlogManager = (BatchlogManagerMBean)JMX.newMBeanProxy(server, ObjectName.getInstance("org.apache.cassandra.db:type=BatchlogManager"), BatchlogManagerMBean.class);
         } catch (MalformedObjectNameException var3) {
            throw new RuntimeException(var3);
         }
      }

      public long getTotalBatchesReplayed() {
         return this.batchlogManager.getTotalBatchesReplayed();
      }
   }
}
