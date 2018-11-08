package com.datastax.bdp.plugin;

import java.lang.reflect.Field;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeferringScheduler extends ScheduledThreadPoolExecutor {
   private long retryDelay;
   private final ThreadPoolExecutor executor;
   private static final Logger logger = LoggerFactory.getLogger(DeferringScheduler.class);

   public DeferringScheduler(ThreadPoolExecutor executor, ThreadFactory threadFactory) {
      super(1, threadFactory);
      this.retryDelay = TimeUnit.MILLISECONDS.toNanos(10L);
      this.executor = executor;
      this.setRemoveOnCancelPolicy(true);
      this.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
      this.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
   }

   protected <V> RunnableScheduledFuture<V> decorateTask(Runnable runnable, RunnableScheduledFuture<V> task) {
      return new DeferringScheduler.DeferringTask(task);
   }

   protected <V> RunnableScheduledFuture<V> decorateTask(Callable<V> callable, RunnableScheduledFuture<V> task) {
      return new DeferringScheduler.DeferringTask(task);
   }

   public long getRetryDelay() {
      return this.retryDelay;
   }

   public void setRetryDelay(long retryDelay) {
      this.retryDelay = retryDelay;
   }

   private void resetTime(RunnableScheduledFuture task) {
      try {
         Field time = task.getClass().getDeclaredField("time");
         time.setAccessible(true);
         time.set(task, Long.valueOf(System.nanoTime() + this.retryDelay));
      } catch (IllegalAccessException | NoSuchFieldException var3) {
         logger.error("Unable to reset the task time", var3);
      }

   }

   public class DeferringTask<V> implements RunnableScheduledFuture<V> {
      private final RunnableScheduledFuture<V> task;

      public DeferringTask(RunnableScheduledFuture<V> this$0) {
         this.task = task;
      }

      public boolean cancel(boolean mayInterruptIfRunning) {
         return this.task.cancel(mayInterruptIfRunning);
      }

      public int compareTo(Delayed o) {
         return this.task.compareTo(o);
      }

      public V get() throws InterruptedException, ExecutionException {
         return this.task.get();
      }

      public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
         return this.task.get(timeout, unit);
      }

      public long getDelay(TimeUnit unit) {
         return this.task.getDelay(unit);
      }

      public boolean isCancelled() {
         return this.task.isCancelled();
      }

      public boolean isDone() {
         return this.task.isDone();
      }

      public boolean isPeriodic() {
         return this.task.isPeriodic();
      }

      public void run() {
         int taskId = DeferringScheduler.logger.isDebugEnabled()?this.hashCode():0;

         try {
            DeferringScheduler.logger.debug("Handing task #{} to executor", Integer.valueOf(taskId));
            DeferringScheduler.this.executor.execute(this.task);
         } catch (RejectedExecutionException var3) {
            DeferringScheduler.logger.debug("Task #{} rejected", Integer.valueOf(taskId));
            if(this.task.isPeriodic()) {
               if(this.task.isCancelled()) {
                  DeferringScheduler.logger.debug("Task #{} dropped", Integer.valueOf(taskId));
               } else {
                  DeferringScheduler.logger.debug("Requeuing task #{}", Integer.valueOf(taskId));
                  DeferringScheduler.this.resetTime(this.task);
                  DeferringScheduler.this.getQueue().add(this);
               }
            } else {
               DeferringScheduler.logger.debug("Cancelling task #{}", Integer.valueOf(taskId));
               this.task.cancel(false);
            }
         }

      }

      public boolean equals(Object obj) {
         return this == obj || obj != null && this.getClass() == obj.getClass() && this.task.equals(((DeferringScheduler.DeferringTask)obj).task);
      }

      public int hashCode() {
         return this.task.hashCode();
      }
   }
}
