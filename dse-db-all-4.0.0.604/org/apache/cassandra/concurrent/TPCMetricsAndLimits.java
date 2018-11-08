package org.apache.cassandra.concurrent;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.concurrent.LongAdder;

public class TPCMetricsAndLimits implements TPCMetrics {
   private final TPCMetricsAndLimits.TaskStats[] stats = new TPCMetricsAndLimits.TaskStats[TPCTaskType.values().length];
   final AtomicLong externallyCountedTasks = new AtomicLong();
   final AtomicLong backpressureCountedLocalTasks = new AtomicLong();
   final AtomicLong backpressureCountedRemoteTasks = new AtomicLong();
   final LongAdder backpressureDelayedTasks = new LongAdder();
   private static final int MAX_REQUESTS_SIZE = 65536;
   int maxConcurrentRequests = DatabaseDescriptor.getTPCConcurrentRequestsLimit();
   int maxPendingRequests = DatabaseDescriptor.getTPCPendingRequestsLimit();

   public TPCMetricsAndLimits() {
      for(int i = 0; i < this.stats.length; ++i) {
         this.stats[i] = new TPCMetricsAndLimits.TaskStats();
      }

   }

   public TPCMetricsAndLimits.TaskStats getTaskStats(TPCTaskType stage) {
      return this.stats[stage.ordinal()];
   }

   public void scheduled(TPCTaskType stage) {
      TPCMetricsAndLimits.TaskStats stat = this.getTaskStats(stage);
      stat.scheduledTasks.add(1L);
      if(stage.externalQueue()) {
         this.externallyCountedTasks.incrementAndGet();
      }

   }

   public void starting(TPCTaskType stage) {
   }

   public void failed(TPCTaskType stage, Throwable t) {
      TPCMetricsAndLimits.TaskStats stat = this.getTaskStats(stage);
      stat.failedTasks.add(1L);
      if(stage.externalQueue()) {
         this.externallyCountedTasks.decrementAndGet();
      }

   }

   public void completed(TPCTaskType stage) {
      TPCMetricsAndLimits.TaskStats stat = this.getTaskStats(stage);
      stat.completedTasks.add(1L);
      if(stage.externalQueue()) {
         this.externallyCountedTasks.decrementAndGet();
      }

   }

   public void cancelled(TPCTaskType stage) {
      this.completed(stage);
   }

   public void pending(TPCTaskType stage, int adjustment) {
      TPCMetricsAndLimits.TaskStats stat = this.getTaskStats(stage);
      stat.pendingTasks.add((long)adjustment);
      if(stage.backpressured() && stage.remote()) {
         this.backpressureCountedRemoteTasks.addAndGet((long)adjustment);
      } else if(stage.backpressured()) {
         this.backpressureCountedLocalTasks.addAndGet((long)adjustment);
      }

   }

   public void blocked(TPCTaskType stage) {
      TPCMetricsAndLimits.TaskStats stat = this.getTaskStats(stage);
      stat.blockedTasks.add(1L);
   }

   public long scheduledTaskCount(TPCTaskType stage) {
      TPCMetricsAndLimits.TaskStats stat = this.getTaskStats(stage);
      return stat.scheduledTasks.longValue();
   }

   public long completedTaskCount(TPCTaskType stage) {
      TPCMetricsAndLimits.TaskStats stat = this.getTaskStats(stage);
      return stat.completedTasks.longValue();
   }

   public long activeTaskCount(TPCTaskType stage) {
      TPCMetricsAndLimits.TaskStats stat = this.getTaskStats(stage);
      return stat.scheduledTasks.longValue() - stat.completedTasks.longValue() - stat.pendingTasks.longValue() - stat.blockedTasks.longValue();
   }

   public long pendingTaskCount(TPCTaskType stage) {
      TPCMetricsAndLimits.TaskStats stat = this.getTaskStats(stage);
      return stat.pendingTasks.longValue();
   }

   public long blockedTaskCount(TPCTaskType stage) {
      TPCMetricsAndLimits.TaskStats stat = this.getTaskStats(stage);
      return stat.blockedTasks.longValue();
   }

   public long backpressureCountedLocalTaskCount() {
      return this.backpressureCountedLocalTasks.get();
   }

   public long backpressureCountedRemoteTaskCount() {
      return this.backpressureCountedRemoteTasks.get();
   }

   public long backpressureCountedTotalTaskCount() {
      return this.backpressureCountedLocalTasks.get() + this.backpressureCountedRemoteTasks.get();
   }

   public void backpressureDelayedTaskCount(int adjustment) {
      this.backpressureDelayedTasks.add((long)adjustment);
   }

   public long backpressureDelayedTaskCount() {
      return this.backpressureDelayedTasks.longValue();
   }

   public int maxQueueSize() {
      long activeReads = this.externallyCountedTasks.get();
      return (int)Math.max((long)this.maxConcurrentRequests - activeReads, 0L);
   }

   public int getMaxPendingRequests() {
      return this.maxPendingRequests;
   }

   public void setMaxPendingRequests(int maxPendingRequests) {
      if(maxPendingRequests > 65536) {
         throw new IllegalArgumentException("Max pending requests must be <= 65536");
      } else {
         this.maxPendingRequests = maxPendingRequests;
      }
   }

   public int getMaxConcurrentRequests() {
      return this.maxConcurrentRequests;
   }

   public void setMaxConcurrentRequests(int maxConcurrentRequests) {
      if(maxConcurrentRequests > 65536) {
         throw new IllegalArgumentException("Max concurrent requests must be <= 65536");
      } else {
         this.maxConcurrentRequests = maxConcurrentRequests;
      }
   }

   static class TaskStats {
      LongAdder scheduledTasks = new LongAdder();
      LongAdder completedTasks = new LongAdder();
      LongAdder failedTasks = new LongAdder();
      LongAdder blockedTasks = new LongAdder();
      LongAdder pendingTasks = new LongAdder();

      TaskStats() {
      }
   }
}
