package com.datastax.bdp.server.system;

import java.lang.management.ManagementFactory;
import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.cassandra.streaming.StreamManagerMBean;

public interface StreamInfoProvider {
   int getCurrentStreamsCount();

   public static class JmxStreamInfoProvider implements StreamInfoProvider {
      private StreamManagerMBean streams;

      public JmxStreamInfoProvider() {
         MBeanServer server = ManagementFactory.getPlatformMBeanServer();

         try {
            this.streams = (StreamManagerMBean)JMX.newMBeanProxy(server, ObjectName.getInstance("org.apache.cassandra.net:type=StreamManager"), StreamManagerMBean.class);
         } catch (MalformedObjectNameException var3) {
            throw new RuntimeException(var3);
         }
      }

      public int getCurrentStreamsCount() {
         return this.streams.getCurrentStreams().size();
      }
   }
}
