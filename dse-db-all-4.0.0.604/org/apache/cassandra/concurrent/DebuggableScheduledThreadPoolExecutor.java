package org.apache.cassandra.concurrent;

import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebuggableScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor {
   private static final Logger logger = LoggerFactory.getLogger(DebuggableScheduledThreadPoolExecutor.class);
   public static final RejectedExecutionHandler rejectedExecutionHandler = new RejectedExecutionHandler() {
      public void rejectedExecution(Runnable task, ThreadPoolExecutor executor) {
         if(executor.isShutdown()) {
            if(!StorageService.instance.isShutdown()) {
               throw new RejectedExecutionException("ScheduledThreadPoolExecutor has shut down.");
            } else {
               if(task instanceof Future) {
                  ((Future)task).cancel(false);
               }

               DebuggableScheduledThreadPoolExecutor.logger.debug("ScheduledThreadPoolExecutor has shut down as part of C* shutdown");
            }
         } else {
            throw new AssertionError("Unknown rejection of ScheduledThreadPoolExecutor task");
         }
      }
   };

   public DebuggableScheduledThreadPoolExecutor(int corePoolSize, String threadPoolName, int priority) {
      super(corePoolSize, new NamedThreadFactory(threadPoolName, priority));
      this.setRejectedExecutionHandler(rejectedExecutionHandler);
      this.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
   }

   public DebuggableScheduledThreadPoolExecutor(int corePoolSize, ThreadFactory threadFactory) {
      super(corePoolSize, threadFactory);
      this.setRejectedExecutionHandler(rejectedExecutionHandler);
      this.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
   }

   public DebuggableScheduledThreadPoolExecutor(String threadPoolName) {
      this(1, threadPoolName, 5);
      this.setRejectedExecutionHandler(rejectedExecutionHandler);
      this.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
   }

   public void afterExecute(Runnable r, Throwable t) {
      super.afterExecute(r, t);
      DebuggableThreadPoolExecutor.logExceptionsAfterExecute(r, t);
   }

   public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
      return super.scheduleAtFixedRate(new DebuggableScheduledThreadPoolExecutor.UncomplainingRunnable(command), initialDelay, period, unit);
   }

   public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
      return super.scheduleWithFixedDelay(new DebuggableScheduledThreadPoolExecutor.UncomplainingRunnable(command), initialDelay, delay, unit);
   }

   private static class UncomplainingRunnable implements Runnable {
      private final Runnable runnable;

      public UncomplainingRunnable(Runnable runnable) {
         this.runnable = runnable;
      }

      public void run() {
         try {
            this.runnable.run();
         } catch (Throwable var2) {
            JVMStabilityInspector.inspectThrowable(var2);
            DebuggableThreadPoolExecutor.handleOrLog(var2);
         }

      }
   }
}
