package org.apache.cassandra.utils.flow;

public abstract class FlowTransform<I, O> extends FlowTransformBase<I, O> implements FlowSubscription {
   protected FlowTransform(Flow<I> source) {
      super(source);
   }

   protected void subscribe(FlowSubscriber<O> subscriber, FlowSubscriptionRecipient subscriptionRecipient) {
      assert this.subscriber == null : "Flow are single-use.";

      this.subscriber = subscriber;
      subscriptionRecipient.onSubscribe(this);
   }

   public void requestFirst(FlowSubscriber<O> subscriber, FlowSubscriptionRecipient subscriptionRecipient) {
      this.subscribe(subscriber, subscriptionRecipient);
      this.sourceFlow.requestFirst(this, this);
   }

   public void onSubscribe(FlowSubscription source) {
      this.source = source;
   }

   public void requestNext() {
      this.source.requestNext();
   }

   public void close() throws Exception {
      this.source.close();
   }
}
