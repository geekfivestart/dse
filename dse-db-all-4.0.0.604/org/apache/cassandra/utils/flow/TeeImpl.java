package org.apache.cassandra.utils.flow;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TeeImpl<T> implements FlowSubscriber<T>, Flow.Tee<T> {
   private final TeeImpl<T>.TeeSubscription[] children;
   private final Flow<T> sourceFlow;
   private volatile FlowSubscription source;
   private final AtomicInteger requests = new AtomicInteger();
   private final AtomicInteger closed = new AtomicInteger();

   TeeImpl(Flow<T> source, int count) {
      this.sourceFlow = source;
      this.children = new TeeImpl.TeeSubscription[count];

      for(int i = 0; i < count; ++i) {
         this.children[i] = new TeeImpl.TeeSubscription();
      }

   }

   public Flow<T> child(int i) {
      return this.children[i];
   }

   public void onSubscribe(FlowSubscription source) {
      this.source = source;
   }

   public void onNext(T item) {
      TeeImpl.TeeSubscription[] var2 = this.children;
      int var3 = var2.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         TeeImpl<T>.TeeSubscription child = var2[var4];
         if(!child.closed) {
            child.subscriber.onNext(item);
         }
      }

   }

   public void onFinal(T item) {
      TeeImpl.TeeSubscription[] var2 = this.children;
      int var3 = var2.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         TeeImpl<T>.TeeSubscription child = var2[var4];
         if(!child.closed) {
            child.subscriber.onFinal(item);
         }
      }

   }

   public void onComplete() {
      TeeImpl.TeeSubscription[] var1 = this.children;
      int var2 = var1.length;

      for(int var3 = 0; var3 < var2; ++var3) {
         TeeImpl<T>.TeeSubscription child = var1[var3];
         if(!child.closed) {
            child.subscriber.onComplete();
         }
      }

   }

   public void onError(Throwable t) {
      TeeImpl.TeeSubscription[] var2 = this.children;
      int var3 = var2.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         TeeImpl<T>.TeeSubscription child = var2[var4];
         if(!child.closed) {
            child.subscriber.onError(t);
         }
      }

   }

   private void requestFirstOne() {
      assert this.source == null;

      if(this.requests.incrementAndGet() >= this.children.length) {
         this.requests.set(0);
         this.sourceFlow.requestFirst(this, this);
      }
   }

   private void requestNextOne() {
      assert this.source != null;

      if(this.requests.incrementAndGet() >= this.children.length) {
         this.requests.set(this.closed.get());
         this.source.requestNext();
      }
   }

   private void closeOne() throws Exception {
      assert this.source != null;

      if(this.closed.incrementAndGet() < this.children.length) {
         this.requestNextOne();
      } else {
         this.source.close();
      }

   }

   public String toString() {
      return Flow.formatTrace("tee " + this.children.length + " ways") + (String)Arrays.stream(this.children).map((child) -> {
         return Flow.formatTrace("tee child", (Object)child.subscriber);
      }).collect(Collectors.joining("\n"));
   }

   class TeeSubscription extends FlowSource<T> {
      volatile boolean closed = false;

      TeeSubscription() {
      }

      public void requestFirst(FlowSubscriber<T> subscriber, FlowSubscriptionRecipient subscriptionRecipient) {
         this.subscribe(subscriber, subscriptionRecipient);
         TeeImpl.this.requestFirstOne();
      }

      public void requestNext() {
         TeeImpl.this.requestNextOne();
      }

      public void close() throws Exception {
         this.closed = true;
         TeeImpl.this.closeOne();
      }
   }
}
