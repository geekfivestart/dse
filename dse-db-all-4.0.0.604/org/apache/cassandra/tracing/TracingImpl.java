package org.apache.cassandra.tracing;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.utils.WrappedRunnable;

class TracingImpl extends Tracing {
   TracingImpl() {
   }

   public void stopSessionImpl() {
      TraceStateImpl state = this.getStateImpl();
      if(state != null) {
         int elapsed = state.elapsed();
         ByteBuffer sessionId = state.sessionIdBytes;
         int ttl = state.ttl;
         state.executeMutation(TraceKeyspace.makeStopSessionMutation(sessionId, elapsed, ttl));
      }
   }

   public TraceState begin(String request, InetAddress client, Map<String, String> parameters) {
      assert isTracing();

      TraceStateImpl state = this.getStateImpl();

      assert state != null;

      long startedAt = System.currentTimeMillis();
      ByteBuffer sessionId = state.sessionIdBytes;
      String command = state.traceType.toString();
      int ttl = state.ttl;
      state.executeMutation(TraceKeyspace.makeStartSessionMutation(sessionId, client, parameters, request, startedAt, command, ttl));
      return state;
   }

   private TraceStateImpl getStateImpl() {
      TraceState state = this.get();
      if(state == null) {
         return null;
      } else {
         if(state instanceof ExpiredTraceState) {
            ExpiredTraceState expiredTraceState = (ExpiredTraceState)state;
            state = expiredTraceState.getDelegate();
         }

         if(state instanceof TraceStateImpl) {
            return (TraceStateImpl)state;
         } else {
            assert false : "TracingImpl states should be of type TraceStateImpl";

            return null;
         }
      }
   }

   protected TraceState newTraceState(InetAddress coordinator, UUID sessionId, Tracing.TraceType traceType) {
      return new TraceStateImpl(coordinator, sessionId, traceType);
   }

   public void trace(final ByteBuffer sessionId, final String message, final int ttl) {
      final String threadName = Thread.currentThread().getName();
      StageManager.tracingExecutor.execute(new WrappedRunnable() {
         public void runMayThrow() {
            TraceStateImpl.mutateWithCatch(TraceKeyspace.makeEventMutation(sessionId, message, -1, threadName, ttl));
         }
      });
   }
}
