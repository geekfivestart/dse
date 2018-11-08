package org.apache.cassandra.utils.flow;

import io.reactivex.functions.Function;

public class FlatMap<I, O> extends Flow.RequestLoopFlow<O> implements FlowSubscription, FlowSubscriber<I> {
   private FlowSubscriber<O> subscriber;
   private final Function<I, Flow<O>> mapper;
   FlowSubscription source;
   private final Flow<I> sourceFlow;
   private final FlatMap<I, O>.FlatMapChild child = new FlatMap.FlatMapChild();
   boolean finalReceived = false;

   public static <I, O> Flow<O> flatMap(Flow<I> source, Function<I, Flow<O>> op) {
      return new FlatMap(op, source);
   }

   FlatMap(Function<I, Flow<O>> mapper, Flow<I> source) {
      this.mapper = mapper;
      this.sourceFlow = source;
   }

   public void requestFirst(FlowSubscriber<O> subscriber, FlowSubscriptionRecipient subscriptionRecipient) {
      assert this.subscriber == null : "Flow are single-use.";

      this.subscriber = subscriber;
      subscriptionRecipient.onSubscribe(this);
      this.sourceFlow.requestFirst(this, this);
   }

   public void onSubscribe(FlowSubscription source) {
      this.source = source;
   }

   public void requestNext() {
      if(!this.child.completeOnNextRequest || this.child.close()) {
         if(this.child.source != null) {
            this.child.source.requestNext();
         } else {
            this.source.requestNext();
         }

      }
   }

   public void close() throws Exception {
      try {
         if(this.child.source != null) {
            this.child.source.close();
         }
      } finally {
         this.source.close();
      }

   }

   public void onNext(I next) {
      Flow flow;
      try {
         flow = (Flow)this.mapper.apply(next);
      } catch (Throwable var4) {
         this.onError(var4);
         return;
      }

      this.child.requestFirst(flow);
   }

   public void onFinal(I next) {
      this.finalReceived = true;
      this.onNext(next);
   }

   public void onError(Throwable throwable) {
      this.subscriber.onError(throwable);
   }

   public void onComplete() {
      this.subscriber.onComplete();
   }

   public String toString() {
      return Flow.formatTrace("flatMap", this.mapper, this.sourceFlow);
   }

   class FlatMapChild implements FlowSubscriber<O> {
      private FlowSubscription source = null;
      boolean completeOnNextRequest = false;

      FlatMapChild() {
      }

      void requestFirst(Flow<O> source) {
         source.requestFirst(this, this);
      }

      public void onSubscribe(FlowSubscription source) {
         this.source = source;
      }

      public void onNext(O next) {
         FlatMap.this.subscriber.onNext(next);
      }

      public void onFinal(O next) {
         if(FlatMap.this.finalReceived) {
            FlatMap.this.subscriber.onFinal(next);
         } else {
            this.completeOnNextRequest = true;
            this.onNext(next);
         }
      }

      public void onError(Throwable throwable) {
         FlatMap.this.subscriber.onError(throwable);
      }

      public boolean close() {
         try {
            this.source.close();
         } catch (Exception var2) {
            FlatMap.this.subscriber.onError(var2);
            return false;
         }

         this.completeOnNextRequest = false;
         this.source = null;
         return true;
      }

      public void onComplete() {
         if(FlatMap.this.finalReceived) {
            FlatMap.this.subscriber.onComplete();
         } else if(this.close()) {
            FlatMap.this.requestInLoop(FlatMap.this);
         }
      }

      public String toString() {
         return FlatMap.this.toString();
      }
   }
}
