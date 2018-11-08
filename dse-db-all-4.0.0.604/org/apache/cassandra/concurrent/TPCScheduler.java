package org.apache.cassandra.concurrent;

import com.google.common.annotations.VisibleForTesting;
import io.reactivex.disposables.Disposable;
import java.util.concurrent.TimeUnit;

public class TPCScheduler extends EventLoopBasedScheduler<TPCEventLoop> {
   @VisibleForTesting
   public TPCScheduler(TPCEventLoop eventLoop) {
      super(eventLoop);
   }

   public boolean canExecuteImmediately(TPCTaskType taskType) {
      return ((TPCEventLoop)this.eventLoop).canExecuteImmediately(taskType);
   }

   public Disposable scheduleDirect(Runnable run, long delay, TimeUnit unit) {
      return super.scheduleDirect(TPCRunnable.wrap(run, delay != 0L?TPCTaskType.TIMED_UNKNOWN:TPCTaskType.UNKNOWN, this.coreId()), delay, unit);
   }

   public Disposable schedule(Runnable run, TPCTaskType stage, long delay, TimeUnit unit) {
      return super.scheduleDirect(TPCRunnable.wrap(run, stage, this.coreId()), delay, unit);
   }

   public boolean isOnScheduler(Thread thread) {
      return thread == ((TPCEventLoop)this.eventLoop).thread();
   }

   public TPCThread thread() {
      return ((TPCEventLoop)this.eventLoop).thread();
   }

   public int coreId() {
      return this.thread().coreId();
   }

   public int metricsCoreId() {
      return this.coreId();
   }

   public String toString() {
      return String.format("TPC scheduler for core %d", new Object[]{Integer.valueOf(this.coreId())});
   }
}
