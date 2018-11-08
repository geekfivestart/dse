package org.apache.cassandra.net;

import java.net.UnknownHostException;
import java.util.Map;

public interface MessagingServiceMBean {
   Map<String, Integer> getLargeMessagePendingTasks();

   Map<String, Long> getLargeMessageCompletedTasks();

   Map<String, Long> getLargeMessageDroppedTasks();

   Map<String, Integer> getSmallMessagePendingTasks();

   Map<String, Long> getSmallMessageCompletedTasks();

   Map<String, Long> getSmallMessageDroppedTasks();

   Map<String, Integer> getGossipMessagePendingTasks();

   Map<String, Long> getGossipMessageCompletedTasks();

   Map<String, Long> getGossipMessageDroppedTasks();

   Map<String, Integer> getDroppedMessages();

   long getTotalTimeouts();

   Map<String, Long> getTimeoutsPerHost();

   Map<String, Double> getBackPressurePerHost();

   void setBackPressureEnabled(boolean var1);

   boolean isBackPressureEnabled();

   /** @deprecated */
   @Deprecated
   int getVersion(String var1) throws UnknownHostException;
}
