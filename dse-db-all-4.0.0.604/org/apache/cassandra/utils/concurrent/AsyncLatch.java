package org.apache.cassandra.utils.concurrent;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public class AsyncLatch {
   private final Runnable task;
   private final Executor executor;
   private volatile long count;
   private static final AtomicLongFieldUpdater<AsyncLatch> countUpdater = AtomicLongFieldUpdater.newUpdater(AsyncLatch.class, "count");

   public AsyncLatch(long count, Runnable task) {
      this(count, task, (Executor)null);
   }

   public AsyncLatch(long count, Runnable task, Executor executor) {
      this.count = count;
      this.task = task;
      this.executor = executor;
   }

   public void countDown() {
      if(countUpdater.decrementAndGet(this) == 0L) {
         if(this.executor == null) {
            this.task.run();
         } else {
            this.executor.execute(this.task);
         }
      }

   }

   public long getCount() {
      return this.count;
   }
}
