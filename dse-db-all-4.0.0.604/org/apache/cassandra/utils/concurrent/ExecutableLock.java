package org.apache.cassandra.utils.concurrent;

import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class ExecutableLock {
   private final Queue<ExecutableLock.AsyncAction> queue = new ConcurrentLinkedQueue();
   private final Semaphore lock;

   public ExecutableLock() {
      this.lock = new Semaphore(1);
   }

   public ExecutableLock(Semaphore lock) {
      this.lock = lock;
   }

   public <T> CompletableFuture<T> execute(Supplier<CompletableFuture<T>> action, Executor executor) {
      CompletableFuture<T> initiator = new CompletableFuture();
      this.tryExecute(initiator, executor);
      return initiator.thenCompose((f) -> {
         return (CompletableFuture)action.get();
      }).whenComplete((result, error) -> {
         this.unlockAndTryNext();
      });
   }

   public <T> T executeBlocking(Callable<T> action) throws Exception {
      this.lock.acquireUninterruptibly();

      Object var2;
      try {
         var2 = action.call();
      } finally {
         this.unlockAndTryNext();
      }

      return var2;
   }

   private <T> void tryExecute(CompletableFuture<T> future, Executor executor) {
      this.queue.add(new ExecutableLock.AsyncAction(future, executor));
      if(this.lock.tryAcquire()) {
         try {
            if(!future.isDone()) {
               future.complete((Object)null);
            } else {
               this.unlockAndTryNext();
            }
         } catch (Exception var4) {
            future.completeExceptionally(var4);
         }
      }

   }

   private void unlockAndTryNext() {
      this.lock.release();
      ExecutableLock.AsyncAction next = (ExecutableLock.AsyncAction)this.queue.poll();

      while(next != null) {
         if(!next.isDone()) {
            next.run();
            next = null;
         } else {
            next = (ExecutableLock.AsyncAction)this.queue.poll();
         }
      }

   }

   private class AsyncAction<T> implements Runnable {
      private final CompletableFuture<T> future;
      private final Executor executor;

      AsyncAction(CompletableFuture<T> var1, Executor future) {
         this.future = future;
         this.executor = executor;
      }

      public void run() {
         this.executor.execute(() -> {
            ExecutableLock.this.tryExecute(this.future, this.executor);
         });
      }

      public boolean isDone() {
         return this.future.isDone();
      }
   }
}
