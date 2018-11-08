package com.datastax.bdp.db.nodesync;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public interface NodeSyncServiceMBean {
   String JMX_GROUP = "com.datastax.nodesync";
   String MBEAN_NAME = String.format("%s:type=%s", new Object[]{"com.datastax.nodesync", "NodeSyncService"});

   boolean enable();

   boolean enable(long var1, TimeUnit var3) throws TimeoutException;

   boolean disable();

   boolean disable(boolean var1, long var2, TimeUnit var4) throws TimeoutException;

   boolean isRunning();

   void setRate(int var1);

   int getRate();

   void startUserValidation(Map<String, String> var1);

   void startUserValidation(String var1, String var2, String var3, String var4, Integer var5);

   void cancelUserValidation(String var1);

   List<Map<String, String>> getRateSimulatorInfo(boolean var1);

   UUID enableTracing();

   UUID enableTracing(Map<String, String> var1);

   UUID currentTracingSession();

   boolean isTracingEnabled();

   void disableTracing();
}
