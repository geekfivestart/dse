package org.apache.cassandra.transport.messages;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import io.netty.buffer.ByteBuf;
import io.reactivex.Single;
import io.reactivex.functions.Function;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.flow.RxThreads;

public class QueryMessage extends Message.Request {
   public static final Message.Codec<QueryMessage> codec = new Message.Codec<QueryMessage>() {
      public QueryMessage decode(ByteBuf body, ProtocolVersion version) {
         String query = CBUtil.readLongString(body);
         return new QueryMessage(query, (QueryOptions)QueryOptions.codec.decode(body, version));
      }

      public void encode(QueryMessage msg, ByteBuf dest, ProtocolVersion version) {
         CBUtil.writeLongString(msg.query, dest);
         if(version == ProtocolVersion.V1) {
            CBUtil.writeConsistencyLevel(msg.options.getConsistency(), dest);
         } else {
            QueryOptions.codec.encode(msg.options, dest, version);
         }

      }

      public int encodedSize(QueryMessage msg, ProtocolVersion version) {
         int size = CBUtil.sizeOfLongString(msg.query);
         if(version == ProtocolVersion.V1) {
            size += CBUtil.sizeOfConsistencyLevel(msg.options.getConsistency());
         } else {
            size += QueryOptions.codec.encodedSize(msg.options, version);
         }

         return size;
      }
   };
   public final String query;
   public final QueryOptions options;

   public QueryMessage(String query, QueryOptions options) {
      super(Message.Type.QUERY);
      this.query = query;
      this.options = options;
   }

   public Single<? extends Message.Response> execute(Single<QueryState> state, long queryStartNanoTime) {
      try {
         UUID tracingID = this.setUpTracing();
         QueryHandler handler = ClientState.getCQLQueryHandler();
         return state.flatMap((s) -> {
            this.checkIsLoggedIn(s);
            Single<ResultMessage> resp = Single.defer(() -> {
               return handler.process(this.query, s, this.options, this.getCustomPayload(), queryStartNanoTime).map((response) -> {
                  if(this.options.skipMetadata() && response instanceof ResultMessage.Rows) {
                     ((ResultMessage.Rows)response).result.metadata.setSkipMetadata();
                  }

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
         if(!(var6 instanceof RequestValidationException) && !(var6 instanceof RequestExecutionException)) {
            logger.error("Unexpected error during query", var6);
         }

         return Tracing.instance.stopSessionAsync().toSingleDefault(ErrorMessage.fromException(var6));
      }
   }

   private UUID setUpTracing() {
      if(!this.shouldTraceRequest()) {
         return null;
      } else {
         UUID sessionId = Tracing.instance.newSession(this.getCustomPayload());
         Builder<String, String> builder = ImmutableMap.builder();
         builder.put("query", this.query);
         if(this.options.getPagingOptions() != null) {
            builder.put("page_size", Integer.toString(this.options.getPagingOptions().pageSize().rawSize()));
         }

         if(this.options.getConsistency() != null) {
            builder.put("consistency_level", this.options.getConsistency().name());
         }

         if(this.options.getSerialConsistency() != null) {
            builder.put("serial_consistency_level", this.options.getSerialConsistency().name());
         }

         Tracing.instance.begin("Execute CQL3 query", this.getClientAddress(), builder.build());
         return sessionId;
      }
   }

   public String toString() {
      return "QUERY " + this.query + (this.options.getPagingOptions() == null?"":"[pageSize = " + this.options.getPagingOptions().pageSize() + "]");
   }
}
