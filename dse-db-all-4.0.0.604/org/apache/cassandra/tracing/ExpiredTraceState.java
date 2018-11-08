package org.apache.cassandra.tracing;

import io.reactivex.Completable;
import org.apache.cassandra.utils.FBUtilities;

class ExpiredTraceState extends TraceState {
   private final TraceState delegate;

   ExpiredTraceState(TraceState delegate) {
      super(FBUtilities.getBroadcastAddress(), delegate.sessionId, delegate.traceType);
      this.delegate = delegate;
   }

   public int elapsed() {
      return -1;
   }

   protected void traceImpl(String message) {
      this.delegate.traceImpl(message);
   }

   protected Completable waitForPendingEvents() {
      return this.delegate.waitForPendingEvents();
   }

   TraceState getDelegate() {
      return this.delegate;
   }
}
