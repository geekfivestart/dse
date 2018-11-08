package org.apache.cassandra.transport.messages;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import io.netty.buffer.ByteBuf;
import io.reactivex.Single;
import io.reactivex.functions.Function;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.exceptions.PreparedQueryNotFoundException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MD5Digest;
import org.apache.cassandra.utils.flow.RxThreads;

public class ExecuteMessage extends Message.Request {
   public static final Message.Codec<ExecuteMessage> codec = new Message.Codec<ExecuteMessage>() {
      public ExecuteMessage decode(ByteBuf body, ProtocolVersion version) {
         MD5Digest statementId = MD5Digest.wrap(CBUtil.readBytes(body));
         MD5Digest resultMetadataId = null;
         if(version.isGreaterOrEqualTo(ProtocolVersion.V5, ProtocolVersion.DSE_V2)) {
            resultMetadataId = MD5Digest.wrap(CBUtil.readBytes(body));
         }

         return new ExecuteMessage(statementId, resultMetadataId, (QueryOptions)QueryOptions.codec.decode(body, version));
      }

      public void encode(ExecuteMessage msg, ByteBuf dest, ProtocolVersion version) {
         CBUtil.writeBytes(msg.statementId.bytes, dest);
         if(version.isGreaterOrEqualTo(ProtocolVersion.V5, ProtocolVersion.DSE_V2)) {
            CBUtil.writeBytes(msg.resultMetadataId.bytes, dest);
         }

         if(version == ProtocolVersion.V1) {
            CBUtil.writeValueList(msg.options.getValues(), dest);
            CBUtil.writeConsistencyLevel(msg.options.getConsistency(), dest);
         } else {
            QueryOptions.codec.encode(msg.options, dest, version);
         }

      }

      public int encodedSize(ExecuteMessage msg, ProtocolVersion version) {
         int size = 0;
         int sizex = size + CBUtil.sizeOfBytes(msg.statementId.bytes);
         if(version.isGreaterOrEqualTo(ProtocolVersion.V5, ProtocolVersion.DSE_V2)) {
            sizex += CBUtil.sizeOfBytes(msg.resultMetadataId.bytes);
         }

         if(version == ProtocolVersion.V1) {
            sizex += CBUtil.sizeOfValueList(msg.options.getValues());
            sizex += CBUtil.sizeOfConsistencyLevel(msg.options.getConsistency());
         } else {
            sizex += QueryOptions.codec.encodedSize(msg.options, version);
         }

         return sizex;
      }
   };
   public final MD5Digest statementId;
   public final MD5Digest resultMetadataId;
   public final QueryOptions options;

   public ExecuteMessage(MD5Digest statementId, MD5Digest resultMetadataId, QueryOptions options) {
      super(Message.Type.EXECUTE);
      this.statementId = statementId;
      this.options = options;
      this.resultMetadataId = resultMetadataId;
   }

   public Single<? extends Message.Response> execute(Single<QueryState> state, long queryStartNanoTime) {
      try {
         QueryHandler handler = ClientState.getCQLQueryHandler();
         ParsedStatement.Prepared prepared = handler.getPrepared(this.statementId);
         if(prepared == null) {
            throw new PreparedQueryNotFoundException(this.statementId);
         } else {
            this.options.prepare(prepared.boundNames);
            UUID tracingID = this.setUpTracing(prepared);
            QueryOptions queryOptions = QueryOptions.addColumnSpecifications(this.options, prepared.boundNames);
            return state.flatMap((s) -> {
               Single<ResultMessage> resp = Single.defer(() -> {
                  this.checkIsLoggedIn(s);
                  return handler.processPrepared(prepared, s, queryOptions, this.getCustomPayload(), queryStartNanoTime).map((response) -> {
                     if(response instanceof ResultMessage.Rows) {
                        ResultMessage.Rows rows = (ResultMessage.Rows)response;
                        ResultSet.ResultMetadata resultMetadata = rows.result.metadata;
                        ProtocolVersion version = this.options.getProtocolVersion();
                        if(version.isGreaterOrEqualTo(ProtocolVersion.V5, ProtocolVersion.DSE_V2)) {
                           if(!prepared.statement.hasConditions()) {
                              if(!resultMetadata.getResultMetadataId().equals(this.resultMetadataId)) {
                                 resultMetadata.setMetadataChanged();
                              } else if(this.options.skipMetadata()) {
                                 resultMetadata.setSkipMetadata();
                              }
                           }
                        } else if(this.options.skipMetadata() && prepared.resultMetadataId.equals(resultMetadata.getResultMetadataId())) {
                           resultMetadata.setSkipMetadata();
                        }
                     }

                     response.setTracingId(tracingID);
                     return response;
                  }).flatMap((response) -> {
                     return Tracing.instance.stopSessionAsync().toSingleDefault(response);
                  });
               });
               return !TPC.isTPCThread()?RxThreads.subscribeOn((Single)resp, TPC.bestTPCScheduler(), TPCTaskType.EXECUTE_STATEMENT):resp;
            });
         }
      } catch (Exception var8) {
         JVMStabilityInspector.inspectThrowable(var8);
         return Single.just(ErrorMessage.fromException(var8));
      }
   }

   private UUID setUpTracing(ParsedStatement.Prepared prepared) {
      if(!this.shouldTraceRequest()) {
         return null;
      } else {
         UUID sessionId = Tracing.instance.newSession(this.getCustomPayload());
         Builder<String, String> builder = ImmutableMap.builder();
         if(this.options.getPagingOptions() != null) {
            builder.put("page_size", Integer.toString(this.options.getPagingOptions().pageSize().rawSize()));
         }

         if(this.options.getConsistency() != null) {
            builder.put("consistency_level", this.options.getConsistency().name());
         }

         if(this.options.getSerialConsistency() != null) {
            builder.put("serial_consistency_level", this.options.getSerialConsistency().name());
         }

         builder.put("query", prepared.rawCQLStatement);

         for(int i = 0; i < prepared.boundNames.size(); ++i) {
            ColumnSpecification cs = (ColumnSpecification)prepared.boundNames.get(i);
            String boundName = cs.name.toString();
            String boundValue = cs.type.asCQL3Type().toCQLLiteral((ByteBuffer)this.options.getValues().get(i), this.options.getProtocolVersion());
            if(boundValue.length() > 1000) {
               boundValue = boundValue.substring(0, 1000) + "...'";
            }

            builder.put("bound_var_" + Integer.toString(i) + "_" + boundName, boundValue);
         }

         Tracing.instance.begin("Execute CQL3 prepared query", this.getClientAddress(), builder.build());
         return sessionId;
      }
   }

   public String toString() {
      return "EXECUTE " + this.statementId + " with " + this.options.getValues().size() + " values at consistency " + this.options.getConsistency();
   }
}
