package com.datastax.bdp.concurrent;

public interface WorkPoolMXBean {
   void setBackPressureThreshold(int var1);

   int getBackPressureThreshold();

   void setFlushMaxTime(int var1);

   int getFlushMaxTime();

   void setConcurrency(int var1) throws Exception;

   int getConcurrency();

   int getMaxConcurrency();

   long[] getQueueSize();

   double getQueueSizeStdDev();

   long getTotalQueueSize();

   long[] getTaskProcessingTimeNanos();

   long[] getProcessedTasks();

   double getBackPressurePauseNanos();

   double getIncomingRate();

   double getOutgoingRate();

   long getThroughput();
}
