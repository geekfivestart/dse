package com.datastax.bdp.node.transport;

import com.google.common.annotations.VisibleForTesting;
import java.util.UUID;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;

public class TraceAwareMessage {
   private boolean hasTraceState;
   private volatile String source;
   private volatile UUID session;
   private volatile String type;

   public TraceAwareMessage() {
      TraceState traceState = Tracing.instance.get();
      if(traceState != null) {
         this.setTraceState(traceState.coordinator.getHostAddress(), traceState.sessionId, traceState.traceType.name());
      }

   }

   @VisibleForTesting
   public final void setTraceState(String source, UUID session, String type) {
      this.hasTraceState = true;
      this.source = source;
      this.session = session;
      this.type = type;
   }

   public boolean hasTraceState() {
      return this.hasTraceState;
   }

   public String getSource() {
      return this.source;
   }

   public UUID getSession() {
      return this.session;
   }

   public String getType() {
      return this.type;
   }
}
