package com.datastax.bdp.db.nodesync;

import java.net.InetAddress;
import java.util.Map;
import java.util.UUID;

public interface NodeSyncServiceProxyMBean {
   String JMX_GROUP = "com.datastax.nodesync";
   String MBEAN_NAME = String.format("%s:type=%s", new Object[]{"com.datastax.nodesync", "NodeSyncServiceProxy"});

   void startUserValidation(InetAddress var1, Map<String, String> var2);

   void cancelUserValidation(InetAddress var1, String var2);

   void enableTracing(InetAddress var1, Map<String, String> var2);

   void disableTracing(InetAddress var1);

   UUID currentTracingSession(InetAddress var1);
}
