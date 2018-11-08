package com.datastax.bdp.server.system;

import java.lang.management.ManagementFactory;
import java.util.Map;
import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.cassandra.net.MessagingServiceMBean;

public interface MessagingInfoProvider {
   Map<String, Integer> getDroppedMessages();

   public static class JmxMessagingInfoProvider implements MessagingInfoProvider {
      private MessagingServiceMBean messaging;

      public JmxMessagingInfoProvider() {
         MBeanServer server = ManagementFactory.getPlatformMBeanServer();

         try {
            this.messaging = (MessagingServiceMBean)JMX.newMBeanProxy(server, ObjectName.getInstance("org.apache.cassandra.net:type=MessagingService"), MessagingServiceMBean.class);
         } catch (MalformedObjectNameException var3) {
            throw new RuntimeException(var3);
         }
      }

      public Map<String, Integer> getDroppedMessages() {
         return this.messaging.getDroppedMessages();
      }
   }
}
