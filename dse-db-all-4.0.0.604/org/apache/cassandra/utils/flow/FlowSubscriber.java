package org.apache.cassandra.utils.flow;

public interface FlowSubscriber<T> extends FlowSubscriptionRecipient {
   void onNext(T var1);

   void onFinal(T var1);

   void onComplete();

   void onError(Throwable var1);
}
