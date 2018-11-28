package org.apache.cassandra.utils.flow;

import io.reactivex.functions.Function;

public class SkipEmpty {
   public SkipEmpty() {
   }

   public static <T> Flow<Flow<T>> skipEmpty(Flow<T> flow) {
      return new SkipEmpty.SkipEmptyContent(flow, (x) -> {
         return x;
      });
   }

   public static <T, U> Flow<U> skipMapEmpty(Flow<T> flow, Function<Flow<T>, U> mapper) {
      return new SkipEmpty.SkipEmptyContent(flow, mapper);
   }

   private static class SkipEmptyContentSubscriber<T> extends FlowTransformNext<T, T> {
      final SkipEmpty.SkipEmptyContent parent;
      T first = null;
      boolean firstIsFinal = false;

      public SkipEmptyContentSubscriber(Flow<T> content, SkipEmpty.SkipEmptyContent parent) {
         super(content);
         this.parent = parent;
      }

      void start() {
         this.sourceFlow.requestFirst(this, this);
      }

      public void onSubscribe(FlowSubscription source) {
         this.source = source;
      }

      public void requestFirst(FlowSubscriber<T> subscriber, FlowSubscriptionRecipient subscriptionRecipient) {
         assert this.first != null;

         assert this.source != null;

         assert this.subscriber == null : "Flow are single-use.";

         this.subscriber = subscriber;
         subscriptionRecipient.onSubscribe(this.source);
         T toReturn = this.first;
         this.first = null;
         if(this.firstIsFinal) {
            subscriber.onFinal(toReturn);
         } else {
            subscriber.onNext(toReturn);
         }

      }

      public void onNext(T item) {
         if(this.subscriber != null) {
            this.subscriber.onNext(item);
         } else {
            if(this.first != null) {
               this.parent.onError(new AssertionError("Got onNext twice with " + this.first + " and then " + item + " in " + this.parent.toString()));
            }

            this.first = item;
            this.parent.onContent(this);
         }

      }

      public void onFinal(T item) {
         if(this.subscriber != null) {
            this.subscriber.onFinal(item);
         } else {
            if(this.first != null) {
               this.parent.onError(new AssertionError("Got onNext twice with " + this.first + " and then " + item + " in " + this.parent.toString()));
            }

            this.first = item;
            this.firstIsFinal = true;
            this.parent.onContent(this);
         }

      }

      public void onComplete() {
         if(this.subscriber != null) {
            this.subscriber.onComplete();
         } else {
            try {
               this.source.close();
            } catch (Exception var2) {
               this.parent.onError(var2);
               return;
            }

            this.parent.onEmpty();
         }

      }

      public void onError(Throwable t) {
         if(this.subscriber != null) {
            this.subscriber.onError(t);
         } else {
            try {
               this.source.close();
            } catch (Throwable var3) {
               t.addSuppressed(var3);
            }

            this.parent.onError(t);
         }

      }

      public String toString() {
         return this.parent.toString();
      }
   }

   static class SkipEmptyContent<T, U> extends Flow<U> {
      final Function<Flow<T>, U> mapper;
      FlowSubscriber<U> subscriber;
      final SkipEmpty.SkipEmptyContentSubscriber<T> child;

      public SkipEmptyContent(Flow<T> content, Function<Flow<T>, U> mapper) {
         this.mapper = mapper;
         this.child = new SkipEmpty.SkipEmptyContentSubscriber(content, this);
      }

      public void requestFirst(FlowSubscriber<U> subscriber, FlowSubscriptionRecipient subscriptionRecipient) {
         assert this.subscriber == null : "Flow are single-use.";

         this.subscriber = subscriber;
         subscriptionRecipient.onSubscribe(FlowSubscription.DONE);
         this.child.start();
      }

      void onContent(Flow<T> child) {
         U result;
         try {
            result = this.mapper.apply(child);
         } catch (Exception var4) {
            this.onError(var4);
            return;
         }

         this.subscriber.onFinal(result);
      }

      void onEmpty() {
         this.subscriber.onComplete();
      }

      void onError(Throwable e) {
         this.subscriber.onError(e);
      }

      public String toString() {
         return Flow.formatTrace("skipEmpty", this.mapper, this.child.sourceFlow);
      }
   }
}
