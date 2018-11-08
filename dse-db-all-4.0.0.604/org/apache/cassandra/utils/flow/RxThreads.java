package org.apache.cassandra.utils.flow;

import io.reactivex.Completable;
import io.reactivex.CompletableObserver;
import io.reactivex.CompletableOperator;
import io.reactivex.Single;
import io.reactivex.SingleObserver;
import io.reactivex.disposables.Disposable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCRunnable;
import org.apache.cassandra.concurrent.TPCTaskType;

public class RxThreads {
   public RxThreads() {
   }

   public static <T> Single<T> subscribeOn(final Single<T> source, final StagedScheduler scheduler, final TPCTaskType taskType) {
      class SubscribeOn extends Single<T> {
         SubscribeOn() {
         }

         protected void subscribeActual(SingleObserver<? super T> subscriber) {
            if(scheduler.canRunDirectly(taskType)) {
               source.subscribe(subscriber);
            } else {
               scheduler.execute(() -> {
                  source.subscribe(subscriber);
               }, taskType);
            }

         }
      }

      return new SubscribeOn();
   }

   public static <T> Single<T> subscribeOnIo(Single<T> source, TPCTaskType taskType) {
      return subscribeOn((Single)source, TPC.ioScheduler(), taskType);
   }

   public static <T> Single<T> subscribeOnCore(Single<T> source, int coreId, TPCTaskType taskType) {
      return subscribeOn((Single)source, TPC.getForCore(coreId), taskType);
   }

   public static Completable subscribeOn(final Completable source, final StagedScheduler scheduler, final TPCTaskType taskType) {
      class SubscribeOn extends Completable {
         SubscribeOn() {
         }

         protected void subscribeActual(CompletableObserver subscriber) {
            if(scheduler.canRunDirectly(taskType)) {
               source.subscribe(subscriber);
            } else {
               scheduler.execute(() -> {
                  source.subscribe(subscriber);
               }, taskType);
            }

         }
      }

      return new SubscribeOn();
   }

   public static Completable subscribeOnIo(Completable source, TPCTaskType taskType) {
      return subscribeOn((Completable)source, TPC.ioScheduler(), taskType);
   }

   private static CompletableOperator awaitAndContinueOnCompletable(StagedScheduler scheduler, TPCTaskType taskType) {
      return (subscriber) -> {
         class AwaitAndContinueOn extends TPCRunnable implements CompletableObserver {
            final CompletableObserver subscriber = subscriber;

            AwaitAndContinueOn() {
               super(subscriber::onComplete, ExecutorLocals.create(), taskType, scheduler.metricsCoreId());
            }

            public void onSubscribe(Disposable disposable) {
               this.subscriber.onSubscribe(disposable);
            }

            public void onComplete() {
               try {
                  scheduler.execute(this);
               } catch (Throwable var2) {
                  this.cancelled();
                  this.subscriber.onError(var2);
               }

            }

            public void onError(Throwable throwable) {
               this.subscriber.onError(throwable);
            }
         }

         return new AwaitAndContinueOn();
      };
   }

   public static Completable awaitAndContinueOn(Completable source, StagedScheduler scheduler, TPCTaskType taskType) {
      return source.lift(awaitAndContinueOnCompletable(scheduler, taskType));
   }

   public static <T> Single<T> singleFromCompletableFuture(final Supplier<CompletableFuture<T>> f) {
      return new Single<T>() {
         protected void subscribeActual(SingleObserver<? super T> observer) {
            ((CompletableFuture)f.get()).whenComplete((v, t) -> {
               if(t != null) {
                  if(t instanceof CompletionException && t.getCause() != null) {
                     t = t.getCause();
                  }

                  observer.onError(t);
               } else {
                  observer.onSuccess(v);
               }

            });
         }
      };
   }
}
