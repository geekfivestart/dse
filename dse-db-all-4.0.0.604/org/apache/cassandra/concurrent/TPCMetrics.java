package org.apache.cassandra.concurrent;

import java.util.concurrent.atomic.AtomicInteger;

public interface TPCMetrics extends TPCLimitsMBean {
   AtomicInteger globallyBackpressuredCores = new AtomicInteger();

   static default void globallyBackpressuredCores(int adjustment) {
      globallyBackpressuredCores.addAndGet(adjustment);
   }

   static default int globallyBackpressuredCores() {
      return globallyBackpressuredCores.get();
   }

   void scheduled(TPCTaskType var1);

   void starting(TPCTaskType var1);

   void failed(TPCTaskType var1, Throwable var2);

   void completed(TPCTaskType var1);

   void cancelled(TPCTaskType var1);

   void pending(TPCTaskType var1, int var2);

   void blocked(TPCTaskType var1);

   long scheduledTaskCount(TPCTaskType var1);

   long completedTaskCount(TPCTaskType var1);

   long activeTaskCount(TPCTaskType var1);

   long pendingTaskCount(TPCTaskType var1);

   long blockedTaskCount(TPCTaskType var1);

   long backpressureCountedLocalTaskCount();

   long backpressureCountedRemoteTaskCount();

   long backpressureCountedTotalTaskCount();

   void backpressureDelayedTaskCount(int var1);

   long backpressureDelayedTaskCount();

   int maxQueueSize();
}
