package org.apache.cassandra.transport.messages;

import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;
import io.reactivex.Single;
import io.reactivex.functions.Function;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.flow.RxThreads;

public class PrepareMessage extends Message.Request {
   public static final Message.Codec<PrepareMessage> codec = new Message.Codec<PrepareMessage>() {
      public PrepareMessage decode(ByteBuf body, ProtocolVersion version) {
         String query = CBUtil.readLongString(body);
         String keyspace = null;
         if(version.isGreaterOrEqualTo(ProtocolVersion.V5, ProtocolVersion.DSE_V2)) {
            int flags = (int)body.readUnsignedInt();
            if((flags & 1) == 1) {
               keyspace = CBUtil.readString(body);
            }
         }

         return new PrepareMessage(query, keyspace);
      }

      public void encode(PrepareMessage msg, ByteBuf dest, ProtocolVersion version) {
         CBUtil.writeLongString(msg.query, dest);
         if(version.isGreaterOrEqualTo(ProtocolVersion.V5, ProtocolVersion.DSE_V2)) {
            if(msg.keyspace == null) {
               dest.writeInt(0);
            } else {
               dest.writeInt(1);
               CBUtil.writeString(msg.keyspace, dest);
            }
         }

      }

      public int encodedSize(PrepareMessage msg, ProtocolVersion version) {
         int size = CBUtil.sizeOfLongString(msg.query);
         if(version.isGreaterOrEqualTo(ProtocolVersion.V5, ProtocolVersion.DSE_V2)) {
            size += 4;
            size += msg.keyspace == null?0:CBUtil.sizeOfString(msg.keyspace);
         }

         return size;
      }
   };
   private final String query;
   private final String keyspace;

   public PrepareMessage(String query, String keyspace) {
      super(Message.Type.PREPARE);
      this.query = query;
      this.keyspace = keyspace;
   }

   public Single<? extends Message.Response> execute(Single<QueryState> state, long queryStartNanoTime) {
      try {
         UUID tracingID = this.setUpTracing();
         QueryHandler handler = ClientState.getCQLQueryHandler();
         return state.flatMap((s) -> {
            this.checkIsLoggedIn(s);
            Single<ResultMessage> resp = Single.defer(() -> {
               return handler.prepare(this.query, s.cloneWithKeyspaceIfSet(this.keyspace), this.getCustomPayload()).map((response) -> {
                  response.setTracingId(tracingID);
                  return response;
               }).flatMap((response) -> {
                  return Tracing.instance.stopSessionAsync().toSingleDefault(response);
               });
            });
            return !TPC.isTPCThread()?RxThreads.subscribeOn((Single)resp, TPC.bestTPCScheduler(), TPCTaskType.EXECUTE_STATEMENT):resp;
         });
      } catch (Exception var6) {
         JVMStabilityInspector.inspectThrowable(var6);
         return Single.just(ErrorMessage.fromException(var6));
      }
   }

   private UUID setUpTracing() {
      if(!this.shouldTraceRequest()) {
         return null;
      } else {
         UUID sessionId = Tracing.instance.newSession(this.getCustomPayload());
         Tracing.instance.begin("Preparing CQL3 query", this.getClientAddress(), ImmutableMap.of("query", this.query));
         return sessionId;
      }
   }

   public String toString() {
      return "PREPARE " + this.query;
   }
}
