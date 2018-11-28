package org.apache.cassandra.net;

import java.util.Iterator;
import java.util.function.Consumer;
import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.apache.cassandra.tracing.Tracing;

abstract class MessageDeliveryTask<T, M extends Message<T>> implements Runnable {
   protected final M message;
   private final long enqueueTime;

   MessageDeliveryTask(M message) {
      this.message = message;
      this.enqueueTime = ApproximateTime.currentTimeMillis();
   }

   static <P, Q> MessageDeliveryTask.RequestDeliveryTask<P, Q> forRequest(Request<P, Q> request) {
      return new MessageDeliveryTask.RequestDeliveryTask(request);
   }

   static <Q> MessageDeliveryTask.ResponseDeliveryTask<Q> forResponse(Response<Q> response) {
      return new MessageDeliveryTask.ResponseDeliveryTask(response);
   }

   public void run() {
      long currentTimeMillis = ApproximateTime.currentTimeMillis();
      if(this.message.verb().droppedGroup() != null) {
         MessagingService.instance().metrics.addQueueWaitTime(this.message.verb().droppedGroup().toString(), currentTimeMillis - this.enqueueTime);
      }

      if(this.message.isTimedOut(currentTimeMillis)) {
         Tracing.trace("Discarding unhandled but timed out message from {}", (Object)this.message.from());
         MessagingService.instance().incrementDroppedMessages(this.message);
      } else {
         this.deliverMessage(currentTimeMillis);
      }
   }

   protected abstract void deliverMessage(long var1);

   private static class ResponseDeliveryTask<Q> extends MessageDeliveryTask<Q, Response<Q>> {
      private ResponseDeliveryTask(Response<Q> response) {
         super(response);
      }

      protected void deliverMessage(long currentTimeMillis) {
         CallbackInfo<Q> info = MessagingService.instance().getRegisteredCallback((Response)this.message, true);
         if(info != null) {
            Tracing.trace("Processing response from {}", (Object)((Response)this.message).from());
            MessageCallback<Q> callback = info.callback;
            if(!((Response)this.message).isFailure()) {
               MessagingService.instance().addLatency(((Response)this.message).verb(), ((Response)this.message).from(), Math.max(currentTimeMillis - info.requestStartMillis, 0L));
            }

            ((Response)this.message).deliverTo(callback);
            MessagingService.instance().updateBackPressureOnReceive(((Response)this.message).from(), ((Response)this.message).verb(), false);
         }
      }
   }

   private static class RequestDeliveryTask<P, Q> extends MessageDeliveryTask<P, Request<P, Q>> {
      private RequestDeliveryTask(Request<P, Q> request) {
         super(request);
      }

      protected void deliverMessage(long currentTimeMillis) {
         Iterator var3 = ((Request)this.message).forwardRequests().iterator();

         while(var3.hasNext()) {
            ForwardRequest<?, ?> forward = (ForwardRequest)var3.next();
            MessagingService.instance().forward(forward);
         }

         if(((Request)this.message).verb().isOneWay()) {
            ((OneWayRequest)this.message).execute();
         } else {
            (this.message).execute(MessagingService.instance()::reply, this::onAborted);
         }

      }

      private void onAborted() {
         Tracing.trace("Discarding partial response to {} (timed out)", (Object)((Request)this.message).from());
         MessagingService.instance().incrementDroppedMessages(this.message);
      }
   }
}
