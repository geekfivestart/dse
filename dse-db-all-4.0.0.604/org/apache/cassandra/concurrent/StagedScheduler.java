package org.apache.cassandra.concurrent;

import io.reactivex.Scheduler;
import io.reactivex.Scheduler.Worker;
import io.reactivex.disposables.Disposable;
import io.reactivex.plugins.RxJavaPlugins;
import java.util.EnumMap;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public abstract class StagedScheduler extends Scheduler {
   private final EnumMap<TPCTaskType, TracingAwareExecutor> executorsForTaskType = new EnumMap(TPCTaskType.class);

   public StagedScheduler() {
   }

   protected abstract boolean isOnScheduler(Thread var1);

   boolean canExecuteImmediately(TPCTaskType taskType) {
      return this.isOnScheduler(Thread.currentThread()) && !taskType.alwaysEnqueue();
   }

   public boolean canRunDirectly(TPCTaskType taskType) {
      return !taskType.logIfExecutedImmediately() && this.canExecuteImmediately(taskType);
   }

   public abstract int metricsCoreId();

   public abstract void enqueue(TPCRunnable var1);

   public void execute(TPCRunnable runnable) {
      if(this.canExecuteImmediately(runnable.taskType())) {
         runnable.run();
      } else {
         this.enqueue(runnable);
      }

   }

   public void execute(Runnable runnable, TPCTaskType taskType) {
      if(!this.canExecuteImmediately(taskType)) {
         this.enqueue(this.wrap(runnable, taskType));
      } else if(taskType.logIfExecutedImmediately()) {
         this.wrap(runnable, taskType).run();
      } else {
         runnable.run();
      }

   }

   public Disposable schedule(Runnable run, TPCTaskType taskType, long delay, TimeUnit unit) {
      return this.scheduleDirect(this.wrap(run, taskType), delay, unit);
   }

   public Disposable scheduleDirect(Runnable run, long delay, TimeUnit unit) {
      Worker w = this.createWorker();
      Runnable decoratedRun = RxJavaPlugins.onSchedule(run);
      StagedScheduler.TPCAwareDisposeTask task = new StagedScheduler.TPCAwareDisposeTask(decoratedRun, w);
      w.schedule(task, delay, unit);
      return task;
   }

   public Disposable schedulePeriodicallyDirect(Runnable run, long initialDelay, long period, TimeUnit unit) {
      throw new UnsupportedOperationException();
   }

   public TPCRunnable wrap(Runnable runnable, TPCTaskType taskType) {
      return TPCRunnable.wrap(runnable, ExecutorLocals.create(), taskType, this.metricsCoreId());
   }

   public TracingAwareExecutor forTaskType(TPCTaskType type) {
      TracingAwareExecutor executor = (TracingAwareExecutor)this.executorsForTaskType.get(type);
      if(executor != null) {
         return executor;
      } else {
         EnumMap var3 = this.executorsForTaskType;
         synchronized(this.executorsForTaskType) {
            return (TracingAwareExecutor)this.executorsForTaskType.computeIfAbsent(type, this::makeExecutor);
         }
      }
   }

   private TracingAwareExecutor makeExecutor(final TPCTaskType type) {
      return new TracingAwareExecutor() {
         public void execute(Runnable runnable, ExecutorLocals locals) {
            StagedScheduler.this.execute(TPCRunnable.wrap(runnable, locals, type, StagedScheduler.this.metricsCoreId()));
         }

         public int coreId() {
            return StagedScheduler.this.metricsCoreId();
         }
      };
   }

   static final class TPCAwareDisposeTask implements Runnable, Callable<Object>, Disposable {
      private final Runnable runnable;
      private final Worker w;
      private final AtomicBoolean hasProgressed;

      TPCAwareDisposeTask(Runnable runnable, Worker w) {
         this.runnable = runnable;
         this.w = w;
         this.hasProgressed = new AtomicBoolean();
      }

      public void run() {
         try {
            if(this.hasProgressed.compareAndSet(false, true)) {
               this.runnable.run();
            }
         } finally {
            this.dispose();
         }

      }

      public Object call() {
         this.run();
         return null;
      }

      public void dispose() {
         this.w.dispose();
         if(this.runnable instanceof TPCRunnable && this.hasProgressed.compareAndSet(false, true)) {
            ((TPCRunnable)this.runnable).cancelled();
         }

      }

      public boolean isDisposed() {
         return this.w.isDisposed();
      }
   }
}
