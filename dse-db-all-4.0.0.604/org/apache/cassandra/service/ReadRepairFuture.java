package org.apache.cassandra.service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.FailureResponse;
import org.apache.cassandra.net.MessageCallback;
import org.apache.cassandra.net.Response;

class ReadRepairFuture extends CompletableFuture<Void> {
   private volatile boolean ready;
   private final AtomicInteger outstandingRepairs = new AtomicInteger();
   private final MessageCallback<EmptyPayload> callback = new MessageCallback<EmptyPayload>() {
      public void onResponse(Response<EmptyPayload> response) {
         ReadRepairFuture.this.onResponse();
      }

      public void onFailure(FailureResponse<EmptyPayload> response) {
      }
   };

   ReadRepairFuture() {
   }

   public Void get() {
      throw new UnsupportedOperationException();
   }

   private void onResponse() {
      if(this.outstandingRepairs.decrementAndGet() == 0 && this.ready) {
         this.complete((Object)null);
      }

   }

   MessageCallback<EmptyPayload> getRepairCallback() {
      assert !this.ready : "onAllRepairSent() has already been called";

      this.outstandingRepairs.incrementAndGet();
      return this.callback;
   }

   void onAllRepairSent() {
      if(!this.ready) {
         this.ready = true;
         if(this.outstandingRepairs.get() == 0) {
            this.complete((Object)null);
         }

      }
   }
}
