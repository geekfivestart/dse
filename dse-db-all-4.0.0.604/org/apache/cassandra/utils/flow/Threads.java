package org.apache.cassandra.utils.flow;

import java.util.EnumMap;
import java.util.concurrent.Callable;
import java.util.function.Function;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCScheduler;
import org.apache.cassandra.concurrent.TPCTaskType;

public class Threads {
   static final EnumMap<TPCTaskType, Flow.Operator[]> REQUEST_ON_CORE = new EnumMap(TPCTaskType.class);
   static final EnumMap<TPCTaskType, Flow.Operator<?, ?>> REQUEST_ON_IO = new EnumMap(TPCTaskType.class);

   public Threads() {
   }

   private static Flow.Operator<Object, Object> constructRequestOnCore(int coreId, TPCTaskType stage) {
      return (source, subscriber, subscriptionRecipient) -> {
         new Threads.RequestOn(source, subscriber, subscriptionRecipient, TPC.getForCore(coreId), stage);
      };
   }

   public static <T> Flow.Operator<T, T> requestOnCore(int coreId, TPCTaskType stage) {
      Flow.Operator[] req = (Flow.Operator[])REQUEST_ON_CORE.computeIfAbsent(stage, (t) -> {
         Flow.Operator<?, ?>[] ops = new Flow.Operator[TPC.getNumCores()];

         for(int i = 0; i < ops.length; ++i) {
            ops[i] = constructRequestOnCore(i, stage);
         }

         return ops;
      });
      return req[coreId];
   }

   public static <T> Flow.Operator<T, T> requestOn(StagedScheduler scheduler, TPCTaskType stage) {
      return scheduler instanceof TPCScheduler?requestOnCore(((TPCScheduler)scheduler).coreId(), stage):(scheduler == TPC.ioScheduler()?requestOnIo(stage):createRequestOn(scheduler, stage));
   }

   private static <T> Flow.Operator<T, T> createRequestOn(StagedScheduler scheduler, TPCTaskType stage) {
      return (source, subscriber, subscriptionRecipient) -> {
         new Threads.RequestOn(source, subscriber, subscriptionRecipient, scheduler, stage);
      };
   }

   private static Flow.Operator<Object, Object> constructRequestOnIO(TPCTaskType stage) {
      return (source, subscriber, subscriptionRecipient) -> {
         new Threads.RequestOn(source, subscriber, subscriptionRecipient, TPC.ioScheduler(), stage);
      };
   }

   public static <T> Flow.Operator<T, T> requestOnIo(TPCTaskType stage) {
      Flow.Operator<T, T> ret = (Flow.Operator)REQUEST_ON_IO.get(stage);
      if(ret != null) {
         return ret;
      } else {
         EnumMap var2 = REQUEST_ON_IO;
         synchronized(REQUEST_ON_IO) {
            return (Flow.Operator)REQUEST_ON_IO.computeIfAbsent(stage, (t) -> {
               return constructRequestOnIO(t);
            });
         }
      }
   }

   public static <T> Flow<T> evaluateOnCore(Callable<T> callable, int coreId, TPCTaskType stage) {
      return new Threads.EvaluateOn(callable, TPC.getForCore(coreId), stage);
   }

   public static <T> Flow<T> evaluateOnIO(Callable<T> callable, TPCTaskType stage) {
      return new Threads.EvaluateOn(callable, TPC.ioScheduler(), stage);
   }

   public static <T> Flow<T> deferOnCore(Callable<Flow<T>> source, int coreId, TPCTaskType stage) {
      return new Threads.DeferOn(source, TPC.getForCore(coreId), stage);
   }

   public static <T> Flow<T> deferOnIO(Callable<Flow<T>> source, TPCTaskType stage) {
      return new Threads.DeferOn(source, TPC.ioScheduler(), stage);
   }

   public static <T> Flow<T> observeOn(Flow<T> source, StagedScheduler scheduler, TPCTaskType taskType) {
      return new Threads.SchedulingTransformer(source, scheduler, taskType);
   }

   static class SchedulingTransformer<I> extends FlowTransformNext<I, I> {
      final StagedScheduler scheduler;
      final TPCTaskType taskType;

      public SchedulingTransformer(Flow<I> source, StagedScheduler scheduler, TPCTaskType taskType) {
         super(source);
         this.scheduler = scheduler;
         this.taskType = taskType;
      }

      public void onNext(I next) {
         if(this.scheduler.canRunDirectly(this.taskType)) {
            this.subscriber.onNext(next);
         } else {
            this.scheduler.execute(() -> {
               this.subscriber.onNext(next);
            }, this.taskType);
         }

      }

      public void onFinal(I next) {
         if(this.scheduler.canRunDirectly(this.taskType)) {
            this.subscriber.onFinal(next);
         } else {
            this.scheduler.execute(() -> {
               this.subscriber.onFinal(next);
            }, this.taskType);
         }

      }

      public String toString() {
         return formatTrace(this.getClass().getSimpleName(), this.scheduler, this.sourceFlow);
      }
   }

   static class DeferOn<T> extends Flow<T> implements Runnable {
      final Callable<Flow<T>> flowSupplier;
      final TPCTaskType taskType;
      final StagedScheduler scheduler;
      FlowSubscriber<T> subscriber;
      FlowSubscriptionRecipient subscriptionRecipient;
      Flow<T> sourceFlow;

      DeferOn(Callable<Flow<T>> source, StagedScheduler scheduler, TPCTaskType taskType) {
         this.flowSupplier = source;
         this.scheduler = scheduler;
         this.taskType = taskType;
      }

      public void requestFirst(FlowSubscriber<T> subscriber, FlowSubscriptionRecipient subscriptionRecipient) {
         this.subscriber = subscriber;
         this.subscriptionRecipient = subscriptionRecipient;
         this.scheduler.execute(this, this.taskType);
      }

      public void run() {
         try {
            this.sourceFlow = (Flow)this.flowSupplier.call();
         } catch (Throwable var2) {
            this.subscriptionRecipient.onSubscribe(FlowSubscription.DONE);
            this.subscriber.onError(var2);
            return;
         }

         this.sourceFlow.requestFirst(this.subscriber, this.subscriptionRecipient);
      }

      public String toString() {
         return Flow.formatTrace("deferOn [" + this.scheduler + "] taskType " + this.taskType, this.flowSupplier, this.sourceFlow);
      }
   }

   static class EvaluateOn<T> extends FlowSource<T> implements Runnable {
      final Callable<T> source;
      final TPCTaskType taskType;
      final StagedScheduler scheduler;

      EvaluateOn(Callable<T> source, StagedScheduler scheduler, TPCTaskType taskType) {
         this.source = source;
         this.scheduler = scheduler;
         this.taskType = taskType;
      }

      public void requestFirst(FlowSubscriber<T> subscriber, FlowSubscriptionRecipient subscriptionRecipient) {
         this.subscribe(subscriber, subscriptionRecipient);
         this.scheduler.execute(this, this.taskType);
      }

      public void requestNext() {
         this.subscriber.onError(new AssertionError("requestNext called after onFinal"));
      }

      public void run() {
         try {
            T v = this.source.call();
            this.subscriber.onFinal(v);
         } catch (Throwable var2) {
            this.subscriber.onError(var2);
         }

      }

      public void close() {
      }

      public String toString() {
         return Flow.formatTrace("evaluateOn [" + this.scheduler + "] taskType " + this.taskType, (Object)this.source);
      }
   }

   static class RequestOn implements FlowSubscription, Runnable, FlowSubscriptionRecipient {
      final StagedScheduler scheduler;
      final TPCTaskType taskType;
      FlowSubscription source;

      <T> RequestOn(Flow<T> source, FlowSubscriber<T> subscriber, FlowSubscriptionRecipient subscriptionRecipient, StagedScheduler scheduler, TPCTaskType taskType) {
         this.scheduler = scheduler;
         this.taskType = taskType;
         subscriptionRecipient.onSubscribe(this);
         if(scheduler.canRunDirectly(taskType)) {
            source.requestFirst(subscriber, this);
         } else {
            scheduler.execute(() -> {
               source.requestFirst(subscriber, this);
            }, taskType);
         }

      }

      public void onSubscribe(FlowSubscription source) {
         this.source = source;
      }

      public void requestNext() {
         this.scheduler.execute(this, this.taskType);
      }

      public void close() throws Exception {
         this.source.close();
      }

      public void run() {
         this.source.requestNext();
      }
   }
}
