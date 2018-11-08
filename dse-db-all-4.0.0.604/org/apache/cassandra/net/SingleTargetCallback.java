package org.apache.cassandra.net;

import java.net.InetAddress;
import java.util.concurrent.CompletableFuture;
import org.apache.cassandra.exceptions.InternalRequestExecutionException;

class SingleTargetCallback<P> extends CompletableFuture<P> implements MessageCallback<P> {
   SingleTargetCallback() {
   }

   public void onResponse(Response<P> response) {
      this.complete(response.payload());
   }

   public void onFailure(FailureResponse response) {
      this.completeExceptionally(new InternalRequestExecutionException(response.reason(), "Got error back from " + response.from()));
   }

   public void onTimeout(InetAddress host) {
      this.completeExceptionally(new CallbackExpiredException());
   }
}
