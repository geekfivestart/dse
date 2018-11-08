package com.datastax.bdp.node.transport;

import com.google.common.net.InetAddresses;
import java.net.InetAddress;
import org.apache.cassandra.tracing.ForwardingTracingImpl;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.tracing.Tracing.TraceType;

public abstract class TraceAwareProcessor<I extends TraceAwareMessage, O> implements ServerProcessor<I, O> {
   public TraceAwareProcessor() {
   }

   public Message<O> process(RequestContext ctx, I message) {
      if(message.hasTraceState()) {
         InetAddress coordinator = InetAddresses.forString(message.getSource());
         TraceType type = TraceType.valueOf(message.getType());
         TraceState state = ForwardingTracingImpl.INSTANCE.newTraceState(coordinator, message.getSession(), type);
         Tracing.instance.set(state);
      }

      Message var9;
      try {
         var9 = this.doProcess(ctx.getId(), message);
      } finally {
         Tracing.instance.set((TraceState)null);
      }

      return var9;
   }

   protected abstract Message<O> doProcess(long var1, I var3);
}
