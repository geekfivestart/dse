package org.apache.cassandra.concurrent;

import java.util.Arrays;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;

public class ExecutorLocals {
   private static final ExecutorLocal<TraceState> tracing;
   private static final ExecutorLocal<ClientWarn.State> clientWarn;
   public final TraceState traceState;
   public final ClientWarn.State clientWarnState;

   private ExecutorLocals(TraceState traceState, ClientWarn.State clientWarnState) {
      this.traceState = traceState;
      this.clientWarnState = clientWarnState;
   }

   public static ExecutorLocals create() {
      TraceState traceState = (TraceState)tracing.get();
      ClientWarn.State clientWarnState = (ClientWarn.State)clientWarn.get();
      return traceState == null && clientWarnState == null?null:new ExecutorLocals(traceState, clientWarnState);
   }

   public static ExecutorLocals create(TraceState traceState, ClientWarn.State clientWarnState) {
      return new ExecutorLocals(traceState, clientWarnState);
   }

   public static void set(ExecutorLocals locals) {
      TraceState traceState = locals == null?null:locals.traceState;
      ClientWarn.State clientWarnState = locals == null?null:locals.clientWarnState;
      tracing.set(traceState);
      clientWarn.set(clientWarnState);
   }

   static {
      tracing = Tracing.instance;
      clientWarn = ClientWarn.instance;

      assert Arrays.equals(ExecutorLocal.all, new ExecutorLocal[]{tracing, clientWarn}) : "ExecutorLocals has not been updated to reflect new ExecutorLocal.all";

   }
}
