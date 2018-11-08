package org.apache.cassandra.utils.flow;

public abstract class FlowTransformBase<I, O> extends Flow.RequestLoopFlow<O> implements FlowSubscriber<I> {
   protected Flow<I> sourceFlow;
   protected FlowSubscriber<O> subscriber;
   protected FlowSubscription source;

   public FlowTransformBase(Flow<I> source) {
      this.sourceFlow = source;
   }

   public void onError(Throwable t) {
      this.subscriber.onError(t);
   }

   public void onComplete() {
      this.subscriber.onComplete();
   }

   public String toString() {
      return formatTrace(this.getClass().getSimpleName(), this.sourceFlow);
   }
}
