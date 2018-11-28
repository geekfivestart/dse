package org.apache.cassandra.concurrent;

import com.google.common.base.Throwables;
import io.reactivex.Completable;
import io.reactivex.CompletableObserver;
import io.reactivex.Single;
import io.reactivex.SingleObserver;
import io.reactivex.functions.Action;
import io.reactivex.functions.Consumer;
import io.reactivex.internal.disposables.EmptyDisposable;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.cassandra.utils.concurrent.CoordinatedAction;
import org.apache.cassandra.utils.concurrent.ExecutableLock;

public class TPCUtils {
   public TPCUtils() {
   }

   public static boolean isWouldBlockException(Throwable t) {
      return Throwables.getRootCause(t) instanceof TPCUtils.WouldBlockException;
   }

   public static <T> T blockingGet(Single<T> single) {
      if(TPC.isTPCThread()) {
         throw new TPCUtils.WouldBlockException("Calling blockingGet would block TPC thread " + Thread.currentThread().getName());
      } else {
         return single.blockingGet();
      }
   }

   public static void blockingAwait(Completable completable) {
      if(TPC.isTPCThread()) {
         throw new TPCUtils.WouldBlockException("Calling blockingAwait would block TPC thread " + Thread.currentThread().getName());
      } else {
         completable.blockingAwait();
      }
   }

   public static <T> T blockingGet(CompletableFuture<T> future) {
      if(TPC.isTPCThread()) {
         throw new TPCUtils.WouldBlockException("Calling blockingGet would block TPC thread " + Thread.currentThread().getName());
      } else {
         try {
            return future.get();
         } catch (Exception var2) {
            throw org.apache.cassandra.utils.Throwables.cleaned(var2);
         }
      }
   }

   public static void blockingAwait(CompletableFuture future) {
      if(TPC.isTPCThread()) {
         throw new TPCUtils.WouldBlockException("Calling blockingAwait would block TPC thread " + Thread.currentThread().getName());
      } else {
         try {
            future.get();
         } catch (Exception var2) {
            throw org.apache.cassandra.utils.Throwables.cleaned(var2);
         }
      }
   }

   public static <T> CompletableFuture<T> toFuture(Single<T> single) {
      CompletableFuture<T> ret = new CompletableFuture();
      single.subscribe(ret::complete, ret::completeExceptionally);
      return ret;
   }

   public static <T> CompletableFuture<Void> toFutureVoid(Single<T> single) {
      CompletableFuture<Void> ret = new CompletableFuture();
      single.subscribe((result) -> {
         ret.complete(null);
      }, ret::completeExceptionally);
      return ret;
   }

   public static CompletableFuture<Void> toFuture(Completable completable) {
      CompletableFuture<Void> ret = new CompletableFuture();
      completable.subscribe(() -> {
         ret.complete(null);
      }, ret::completeExceptionally);
      return ret;
   }

   public static Completable toCompletable(final CompletableFuture<Void> future) {
      return new Completable() {
         protected void subscribeActual(CompletableObserver observer) {
            observer.onSubscribe(EmptyDisposable.INSTANCE);
            future.whenComplete((res, err) -> {
               if(err == null) {
                  observer.onComplete();
               } else {
                  observer.onError(org.apache.cassandra.utils.Throwables.unwrapped(err));
               }

            });
         }
      };
   }

   public static <T> Single<T> toSingle(final CompletableFuture<T> future) {
      return new Single<T>() {
         protected void subscribeActual(SingleObserver<? super T> observer) {
            observer.onSubscribe(EmptyDisposable.INSTANCE);
            future.whenComplete((res, err) -> {
               if(err == null) {
                  observer.onSuccess(res);
               } else {
                  observer.onError(org.apache.cassandra.utils.Throwables.unwrapped(err));
               }

            });
         }
      };
   }

   public static CompletableFuture<Void> completedFuture() {
      return CompletableFuture.completedFuture(null);
   }

   public static <T> CompletableFuture<T> completedFuture(T value) {
      return CompletableFuture.completedFuture(value);
   }

   public static <T> CompletableFuture<T> completableFuture(Callable<T> callable, ExecutorService executor) {
      assert callable != null : "Received null callable";

      assert executor != null : "Received null executor";

      return CompletableFuture.supplyAsync(() -> {
         try {
            return callable.call();
         } catch (Exception var2) {
            throw new CompletionException(var2);
         }
      }, executor);
   }

   public static <T> CompletableFuture<Void> completableFutureVoid(Callable<T> callable, ExecutorService executor) {
      assert callable != null : "Received null callable";

      assert executor != null : "Received null executor";

      return CompletableFuture.supplyAsync(() -> {
         try {
            callable.call();
            return null;
         } catch (Exception var2) {
            throw new CompletionException(var2);
         }
      }, executor);
   }

   public static <T> CompletableFuture<T> withLock(ExecutableLock lock, Supplier<CompletableFuture<T>> action) {
      return lock.execute(action, TPC.bestTPCScheduler().getExecutor());
   }

   public static <T> T withLockBlocking(ExecutableLock lock, Callable<T> action) {
      try {
         return lock.executeBlocking(action);
      } catch (Exception var3) {
         throw org.apache.cassandra.utils.Throwables.cleaned(var3);
      }
   }

   public static <T> CompletableFuture<T> withLocks(SortedMap<Long, ExecutableLock> locks, long startTimeMillis, long timeoutMillis, Supplier<CompletableFuture<T>> onLocksAcquired, Function<TimeoutException, RuntimeException> onTimeout) {
      CompletableFuture first;
      CoordinatedAction<T> coordinator = new CoordinatedAction<T>(onLocksAcquired, locks.size(), startTimeMillis, timeoutMillis, TimeUnit.MILLISECONDS);
      CompletableFuture prev = first = new CompletableFuture();
      CompletableFuture<T> result = null;
      for (ExecutableLock lock : locks.values()) {
         CompletableFuture current = new CompletableFuture();
         result = prev.thenCompose(ignored -> lock.execute(() -> {
            current.complete(null);
            return coordinator.get();
         }, TPC.bestTPCScheduler().getExecutor()));
         prev = current;
      }
      first.complete(null);
      result = result.exceptionally(t -> {
         if (t instanceof CompletionException && t.getCause() != null) {
            t = t.getCause();
         }
         if (t instanceof TimeoutException) {
            throw (RuntimeException)onTimeout.apply((TimeoutException)t);
         }
         throw Throwables.propagate((Throwable)t);
      });
      return result;
   }

   public static final class WouldBlockException extends RuntimeException {
      public WouldBlockException(String message) {
         super(message);
      }
   }
}
