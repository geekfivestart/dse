package org.apache.cassandra.service;

public interface GCInspectorMXBean {
   double[] getAndResetStats();

   void setGcWarnThresholdInMs(long var1);

   long getGcWarnThresholdInMs();

   void setGcLogThresholdInMs(long var1);

   long getGcLogThresholdInMs();

   long getStatusThresholdInMs();
}
