package org.apache.cassandra.utils.flow;

public abstract class FlowSource<T> extends Flow<T> implements FlowSubscription {
   protected FlowSubscriber<T> subscriber;

   public FlowSource() {
   }

   public void subscribe(FlowSubscriber<T> subscriber, FlowSubscriptionRecipient subscriptionRecipient) {
      assert this.subscriber == null : "Flow are single-use.";

      this.subscriber = subscriber;
      subscriptionRecipient.onSubscribe(this);
   }

   public void requestFirst(FlowSubscriber<T> subscriber, FlowSubscriptionRecipient subscriptionRecipient) {
      this.subscribe(subscriber, subscriptionRecipient);
      this.requestNext();
   }

   public String toString() {
      return formatTrace(this.getClass().getSimpleName());
   }
}
