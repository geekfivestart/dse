package org.apache.cassandra.utils.flow;

public abstract class FlowTransformNext<I, O> extends FlowTransformBase<I, O> {
   protected FlowSubscriptionRecipient subscriptionRecipient;

   protected FlowTransformNext(Flow<I> source) {
      super(source);
   }

   protected void subscribe(FlowSubscriber<O> subscriber, FlowSubscriptionRecipient subscriptionRecipient) {
      assert this.subscriber == null : "Flow are single-use.";

      this.subscriber = subscriber;
      this.subscriptionRecipient = subscriptionRecipient;
   }

   public void requestFirst(FlowSubscriber<O> subscriber, FlowSubscriptionRecipient subscriptionRecipient) {
      this.subscribe(subscriber, subscriptionRecipient);
      this.sourceFlow.requestFirst(this, this);
   }

   public void onSubscribe(FlowSubscription source) {
      this.source = source;
      this.subscriptionRecipient.onSubscribe(source);
   }
}
