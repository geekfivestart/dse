package org.apache.cassandra.tracing;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import org.apache.cassandra.tracing.Tracing.TraceType;

public class ForwardingTracingImpl extends Tracing {
   public static final ForwardingTracingImpl INSTANCE = new ForwardingTracingImpl();

   private ForwardingTracingImpl() {
   }

   public TraceState begin(String request, InetAddress client, Map<String, String> parameters) {
      return instance.begin(request, client, parameters);
   }

   public TraceState newTraceState(InetAddress coordinator, UUID sessionId, TraceType traceType) {
      return instance.newTraceState(coordinator, sessionId, traceType);
   }

   public void trace(ByteBuffer sessionId, String message, int ttl) {
      instance.trace(sessionId, message, ttl);
   }

   protected void stopSessionImpl() {
      instance.stopSessionImpl();
   }
}
