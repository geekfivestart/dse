package org.apache.cassandra.utils.concurrent;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.cassandra.utils.SystemTimeSource;
import org.apache.cassandra.utils.TimeSource;

public class CoordinatedAction<T> implements Supplier<CompletableFuture<T>> {
   final Set<CompletableFuture<T>> futures;
   final Lock lock;
   final Supplier<CompletableFuture<T>> action;
   final CountDownLatch latch;
   final TimeSource source;
   final long timeoutInMillis;
   final long startTimeInMillis;

   public CoordinatedAction(Supplier<CompletableFuture<T>> action, int parties, long startTimeInMillis, long timeout, TimeUnit unit) {
      this(action, parties, startTimeInMillis, timeout, unit, new SystemTimeSource());
   }

   @VisibleForTesting
   CoordinatedAction(Supplier<CompletableFuture<T>> action, int parties, long startTimeInMillis, long timeout, TimeUnit unit, TimeSource source) {
      this.futures = Sets.newConcurrentHashSet();
      this.lock = new ReentrantLock();
      this.latch = new CountDownLatch(parties);
      this.action = action;
      this.source = source;
      this.startTimeInMillis = startTimeInMillis;
      this.timeoutInMillis = TimeUnit.MILLISECONDS.convert(timeout, unit);
   }

   public CompletableFuture<T> get() {
      try {
         CompletableFuture future;
         if(this.source.currentTimeMillis() - this.startTimeInMillis < this.timeoutInMillis) {
            future = new CompletableFuture();
            this.futures.add(future);
            this.latch.countDown();
            if(this.latch.await(0L, TimeUnit.MILLISECONDS) && this.lock.tryLock()) {
               try {
                  if(this.futures.stream().noneMatch((f) -> {
                     return f.isCompletedExceptionally();
                  })) {
                     ((CompletableFuture)this.action.get()).whenComplete((r, e) -> {
                        if(e == null) {
                           this.futures.stream().forEach((f) -> {
                              f.complete(r);
                           });
                        } else {
                           this.futures.stream().forEach((f) -> {
                              f.completeExceptionally(e);
                           });
                        }

                     });
                  } else {
                     this.futures.stream().forEach((f) -> {
                        f.completeExceptionally(new TimeoutException());
                     });
                  }
               } finally {
                  this.lock.unlock();
               }
            }

            return future;
         } else {
            future = new CompletableFuture();
            this.futures.add(future);
            this.futures.stream().forEach((f) -> {
               f.completeExceptionally(new TimeoutException());
            });
            this.latch.countDown();
            return future;
         }
      } catch (InterruptedException var6) {
         CompletableFuture<T> future = new CompletableFuture();
         future.completeExceptionally(var6);
         return future;
      }
   }
}
