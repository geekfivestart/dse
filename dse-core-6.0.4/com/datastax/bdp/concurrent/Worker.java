package com.datastax.bdp.concurrent;

import com.datastax.bdp.system.TimeSource;
import com.google.common.base.Preconditions;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Worker implements Runnable {
   private static final Logger logger = LoggerFactory.getLogger(Worker.class);
   private static final ThreadLocal<Worker.Context> currentWorkerContext = ThreadLocal.withInitial(Worker.Context::<init>);
   private final BlockingQueue<Task> queue;
   private final TimeSource timeSource;
   private volatile long latestTaskCreateTime;
   private volatile long latestTaskStartTime;
   private volatile long latestTaskEndTime;
   private volatile long latestTaskProcessingTime;
   private final AtomicLong processedTasks;
   private final Worker.TaskListener listener;

   public Worker(TimeSource timeSource, BlockingQueue<Task> queue) {
      this(timeSource, queue, new Worker.NoOpListener());
   }

   public Worker(TimeSource timeSource, BlockingQueue<Task> queue, Worker.TaskListener listener) {
      Preconditions.checkNotNull(queue);
      Preconditions.checkNotNull(listener);
      Preconditions.checkNotNull(timeSource);
      this.queue = queue;
      this.timeSource = timeSource;
      this.latestTaskCreateTime = this.latestTaskStartTime = this.latestTaskEndTime = timeSource.nanoTime();
      this.listener = listener;
      this.processedTasks = new AtomicLong();
   }

   public static Worker.Context getCurrentWorkerContext() {
      return (Worker.Context)currentWorkerContext.get();
   }

   public void run() {
      Worker.Context context = (Worker.Context)currentWorkerContext.get();
      context.isWorkerThread = true;
      boolean shutdown = false;

      while(!shutdown) {
         Task task = null;
         int workUnits = 0;
         boolean completed = false;

         try {
            task = (Task)this.queue.take();
            context.currentTaskEpoch = task.getEpoch();
            this.latestTaskCreateTime = task.getTimestampNanos();
            this.latestTaskStartTime = this.timeSource.nanoTime();
            workUnits = task.run();
            completed = true;
         } catch (FlushTask.FlushedQueueException var11) {
            shutdown = var11.task().isShutdown();
            if(shutdown) {
               logger.info("Shutting down work pool worker!");
            }
         } catch (Throwable var12) {
            logger.error(var12.getMessage(), var12);
         } finally {
            if(task != null) {
               task.signal();
               this.latestTaskEndTime = this.timeSource.nanoTime();
               this.latestTaskProcessingTime = this.latestTaskEndTime - this.latestTaskCreateTime;
               this.processedTasks.incrementAndGet();
               if(completed) {
                  this.listener.onComplete(this, task, workUnits);
               }
            }

         }
      }

   }

   public long getTaskProcessingTimeNanos() {
      long now = this.timeSource.nanoTime();
      long processingTime;
      if(this.latestTaskEndTime - this.latestTaskStartTime < 0L && now - this.latestTaskCreateTime > this.latestTaskProcessingTime) {
         processingTime = now - this.latestTaskCreateTime;
      } else {
         processingTime = this.latestTaskProcessingTime;
      }

      return processingTime;
   }

   public long getProcessedTasks() {
      return this.processedTasks.get();
   }

   private static class NoOpListener implements Worker.TaskListener {
      private NoOpListener() {
      }

      public void onComplete(Worker w, Task t, int workUnits) {
      }
   }

   public static class Context {
      public long currentTaskEpoch;
      public boolean isWorkerThread;

      public Context() {
      }
   }

   public interface TaskListener {
      void onComplete(Worker var1, Task var2, int var3);
   }
}
