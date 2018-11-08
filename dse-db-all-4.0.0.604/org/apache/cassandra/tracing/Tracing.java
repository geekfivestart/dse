package org.apache.cassandra.tracing;

import io.netty.util.concurrent.FastThreadLocal;
import io.reactivex.Completable;
import io.reactivex.functions.Action;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.cassandra.concurrent.ExecutorLocal;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.metrics.TracingMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.UUIDSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Tracing implements ExecutorLocal<TraceState> {
   protected static final Logger logger = LoggerFactory.getLogger(Tracing.class);
   private static final FastThreadLocal<TraceState> state = new FastThreadLocal();
   private static final InetAddress localAddress = FBUtilities.getLocalAddress();
   private static final TracingMetrics metrics = new TracingMetrics();
   protected static final ConcurrentMap<UUID, TraceState> sessions = new ConcurrentHashMap();
   public static final Tracing instance;

   public Tracing() {
   }

   public void onDroppedTask() {
      metrics.droppedTasks.mark();
   }

   public UUID getSessionId() {
      assert isTracing();

      return ((TraceState)state.get()).sessionId;
   }

   public Tracing.TraceType getTraceType() {
      assert isTracing();

      return ((TraceState)state.get()).traceType;
   }

   public int getTTL() {
      assert isTracing();

      return ((TraceState)state.get()).ttl;
   }

   public static boolean isTracing() {
      return instance.get() != null;
   }

   public UUID newSession(Map<String, ByteBuffer> customPayload) {
      return this.newSession(UUIDGen.getTimeUUID(), Tracing.TraceType.QUERY, customPayload);
   }

   public UUID newSession(Tracing.TraceType traceType) {
      return this.newSession(UUIDGen.getTimeUUID(), traceType, Collections.emptyMap());
   }

   public UUID newSession(UUID sessionId, Tracing.TraceType traceType) {
      return this.newSession(sessionId, traceType, Collections.emptyMap());
   }

   protected UUID newSession(UUID sessionId, Tracing.TraceType traceType, Map<String, ByteBuffer> customPayload) {
      assert this.get() == null;

      TraceState ts = this.newTraceState(localAddress, sessionId, traceType);
      this.set(ts);
      sessions.put(sessionId, ts);
      return sessionId;
   }

   public void doneWithNonLocalSession(TraceState state) {
      if(state.releaseReference() == 0) {
         sessions.remove(state.sessionId);
      }

   }

   public void stopSession() {
      this.stopSessionAsync().blockingAwait();
   }

   public Completable stopSessionAsync() {
      TraceState state = this.get();
      return state == null?Completable.complete():Completable.fromRunnable(this::stopSessionImpl).andThen(state.stop()).doOnComplete(() -> {
         sessions.remove(state.sessionId);
         this.set((TraceState)null);
      });
   }

   protected abstract void stopSessionImpl();

   public TraceState get() {
      return (TraceState)state.get();
   }

   public TraceState get(UUID sessionId) {
      return (TraceState)sessions.get(sessionId);
   }

   public void set(TraceState tls) {
      state.set(tls);
   }

   public TraceState begin(String request, Map<String, String> parameters) {
      return this.begin(request, (InetAddress)null, parameters);
   }

   public abstract TraceState begin(String var1, InetAddress var2, Map<String, String> var3);

   public TraceState initializeFromMessage(Message message) {
      if(!message.isTraced()) {
         return null;
      } else {
         Tracing.SessionInfo info = message.tracingInfo();
         TraceState ts = this.get(info.sessionId);
         if(ts != null && ts.acquireReference()) {
            return ts;
         } else {
            ts = this.newTraceState(message.from(), info.sessionId, info.traceType);
            if(message.isRequest()) {
               sessions.put(info.sessionId, ts);
               return ts;
            } else {
               return new ExpiredTraceState(ts);
            }
         }
      }
   }

   public void onMessageSend(Message message, int messageSerializedSize) {
      if(message.isTraced()) {
         Tracing.SessionInfo info = message.tracingInfo();
         TraceState state = this.get(info.sessionId);
         if(state == null) {
            state = this.newTraceState(message.from(), info.sessionId, info.traceType);
         }

         state.trace(String.format("Sending %s message to %s, size=%s bytes", new Object[]{message.verb(), message.to(), Integer.valueOf(messageSerializedSize)}));
         if(!message.isRequest()) {
            instance.doneWithNonLocalSession(state);
         }

      }
   }

   public Tracing.SessionInfo sessionInfo() {
      TraceState state = this.get();

      assert state != null;

      return new Tracing.SessionInfo(state);
   }

   protected abstract TraceState newTraceState(InetAddress var1, UUID var2, Tracing.TraceType var3);

   public static void traceRepair(String format, Object... args) {
      TraceState state = instance.get();
      if(state != null) {
         state.trace(format, args);
      }
   }

   public static void trace(String message) {
      TraceState state = instance.get();
      if(state != null) {
         state.trace(message);
      }
   }

   public static void trace(String format, Object arg) {
      TraceState state = instance.get();
      if(state != null) {
         state.trace(format, arg);
      }
   }

   public static void trace(String format, Object arg1, Object arg2) {
      TraceState state = instance.get();
      if(state != null) {
         state.trace(format, arg1, arg2);
      }
   }

   public static void trace(String format, Object... args) {
      TraceState state = instance.get();
      if(state != null) {
         state.trace(format, args);
      }
   }

   public abstract void trace(ByteBuffer var1, String var2, int var3);

   static {
      Tracing tracing = null;
      String customTracingClass = System.getProperty("cassandra.custom_tracing_class");
      if(null != customTracingClass) {
         try {
            tracing = (Tracing)FBUtilities.construct(customTracingClass, "Tracing");
            logger.info("Using {} as tracing queries (as requested with -Dcassandra.custom_tracing_class)", customTracingClass);
         } catch (Exception var3) {
            JVMStabilityInspector.inspectThrowable(var3);
            logger.error(String.format("Cannot use class %s for tracing, ignoring by defaulting to normal tracing", new Object[]{customTracingClass}), var3);
         }
      }

      instance = (Tracing)(null != tracing?tracing:new TracingImpl());
   }

   public static class SessionInfo {
      public final UUID sessionId;
      public final Tracing.TraceType traceType;

      private SessionInfo(TraceState state) {
         this(state.sessionId, state.traceType);
      }

      public SessionInfo(UUID sessionId, Tracing.TraceType traceType) {
         this.sessionId = sessionId;
         this.traceType = traceType;
      }

      public void serialize(DataOutputPlus out) throws IOException {
         UUIDSerializer.serializer.serialize(this.sessionId, out);
         out.writeByte(Tracing.TraceType.serialize(this.traceType));
      }

      public long serializedSize() {
         return UUIDSerializer.serializer.serializedSize(this.sessionId) + 1L;
      }

      public static Tracing.SessionInfo deserialize(DataInputPlus in) throws IOException {
         UUID sessionId = UUIDSerializer.serializer.deserialize(in);
         Tracing.TraceType traceType = Tracing.TraceType.deserialize((byte)in.readUnsignedByte());
         return new Tracing.SessionInfo(sessionId, traceType);
      }
   }

   public static enum TraceType {
      NONE,
      QUERY,
      REPAIR,
      NODESYNC;

      private static final Tracing.TraceType[] ALL_VALUES = values();
      private static final int[] TTLS = new int[]{DatabaseDescriptor.getTracetypeQueryTTL(), DatabaseDescriptor.getTracetypeQueryTTL(), DatabaseDescriptor.getTracetypeRepairTTL(), DatabaseDescriptor.getTracetypeNodeSyncTTL()};

      private TraceType() {
      }

      public static Tracing.TraceType deserialize(byte b) {
         return b >= 0 && ALL_VALUES.length > b?ALL_VALUES[b]:NONE;
      }

      public static byte serialize(Tracing.TraceType value) {
         return (byte)value.ordinal();
      }

      public int getTTL() {
         return TTLS[this.ordinal()];
      }
   }
}
