package org.apache.cassandra.net;

import java.net.InetAddress;
import org.apache.cassandra.concurrent.TracingAwareExecutor;

class CallbackInfo<Q> {
   final InetAddress target;
   final MessageCallback<Q> callback;
   final Verb<?, Q> verb;
   final TracingAwareExecutor responseExecutor;
   final long requestStartMillis;

   CallbackInfo(InetAddress target, MessageCallback<Q> callback, Verb<?, Q> verb, TracingAwareExecutor responseExecutor, long requestStartMillis) {
      this.target = target;
      this.callback = callback;
      this.verb = verb;
      this.responseExecutor = responseExecutor;
      this.requestStartMillis = requestStartMillis;
   }

   public String toString() {
      return String.format("CallbackInfo(target=%s, callback=%s, verb=%s)", new Object[]{this.target, this.callback, this.verb});
   }
}
