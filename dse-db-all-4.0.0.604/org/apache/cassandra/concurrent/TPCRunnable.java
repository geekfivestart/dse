package org.apache.cassandra.concurrent;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public class TPCRunnable implements Runnable {
   private static final AtomicIntegerFieldUpdater<TPCRunnable> COMPLETION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(TPCRunnable.class, "completed");
   private final Runnable runnable;
   private final ExecutorLocals locals;
   private final TPCTaskType taskType;
   private final TPCMetrics metrics;
   private volatile int completed;

   public TPCRunnable(Runnable runnable, ExecutorLocals locals, TPCTaskType taskType, int scheduledOn) {
      this.runnable = runnable;
      this.locals = locals;
      this.taskType = taskType;
      this.metrics = TPC.metrics(scheduledOn);
      this.metrics.scheduled(taskType);
   }

   public void run() {
      this.metrics.starting(this.taskType);
      ExecutorLocals.set(this.locals);

      try {
         this.runnable.run();
      } catch (Throwable var5) {
         this.metrics.failed(this.taskType, var5);
         throw var5;
      } finally {
         if(!COMPLETION_UPDATER.compareAndSet(this, 0, 1)) {
            throw new IllegalStateException("TPC task was cancelled while still running.");
         }

         this.metrics.completed(this.taskType);
      }

   }

   public void cancelled() {
      if(COMPLETION_UPDATER.compareAndSet(this, 0, 1)) {
         this.metrics.cancelled(this.taskType);
      }

   }

   public void setPending() {
      this.metrics.pending(this.taskType, 1);
   }

   public void unsetPending() {
      this.metrics.pending(this.taskType, -1);
   }

   public boolean isPendable() {
      return this.taskType.pendable();
   }

   public boolean hasPriority() {
      return this.taskType.priority();
   }

   public boolean alwaysEnqueue() {
      return this.taskType.alwaysEnqueue();
   }

   public TPCTaskType taskType() {
      return this.taskType;
   }

   public void blocked() {
      this.metrics.blocked(this.taskType);
   }

   public static TPCRunnable wrap(Runnable runnable) {
      return wrap(runnable, ExecutorLocals.create(), TPCTaskType.UNKNOWN, TPC.getNumCores());
   }

   public static TPCRunnable wrap(Runnable runnable, int defaultCore) {
      return wrap(runnable, ExecutorLocals.create(), TPCTaskType.UNKNOWN, defaultCore);
   }

   public static TPCRunnable wrap(Runnable runnable, TPCTaskType defaultStage, int defaultCore) {
      return wrap(runnable, ExecutorLocals.create(), defaultStage, defaultCore);
   }

   public static TPCRunnable wrap(Runnable runnable, TPCTaskType defaultStage, StagedScheduler scheduler) {
      return wrap(runnable, ExecutorLocals.create(), defaultStage, scheduler.metricsCoreId());
   }

   public static TPCRunnable wrap(Runnable runnable, ExecutorLocals locals, TPCTaskType defaultStage, StagedScheduler scheduler) {
      return wrap(runnable, locals, defaultStage, scheduler.metricsCoreId());
   }

   public static TPCRunnable wrap(Runnable runnable, ExecutorLocals locals, TPCTaskType defaultStage, int defaultCore) {
      return runnable instanceof TPCRunnable?(TPCRunnable)runnable:new TPCRunnable(runnable, locals, defaultStage, defaultCore);
   }
}
