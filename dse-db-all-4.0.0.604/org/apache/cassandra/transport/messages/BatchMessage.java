package org.apache.cassandra.transport.messages;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import io.netty.buffer.ByteBuf;
import io.reactivex.Single;
import io.reactivex.functions.Function;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.PreparedQueryNotFoundException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MD5Digest;
import org.apache.cassandra.utils.flow.RxThreads;

public class BatchMessage extends Message.Request {
   public static final Message.Codec<BatchMessage> codec = new Message.Codec<BatchMessage>() {
      public BatchMessage decode(ByteBuf body, ProtocolVersion version) {
         byte type = body.readByte();
         int n = body.readUnsignedShort();
         List<Object> queryOrIds = new ArrayList(n);
         List<List<ByteBuffer>> variables = new ArrayList(n);

         for(int i = 0; i < n; ++i) {
            byte kind = body.readByte();
            if(kind == 0) {
               queryOrIds.add(CBUtil.readLongString(body));
            } else {
               if(kind != 1) {
                  throw new ProtocolException("Invalid query kind in BATCH messages. Must be 0 or 1 but got " + kind);
               }

               queryOrIds.add(MD5Digest.wrap(CBUtil.readBytes(body)));
            }

            variables.add(CBUtil.readValueList(body, version));
         }

         QueryOptions options = (QueryOptions)QueryOptions.codec.decode(body, version);
         return new BatchMessage(this.toType(type), queryOrIds, variables, options);
      }

      public void encode(BatchMessage msg, ByteBuf dest, ProtocolVersion version) {
         int queries = msg.queryOrIdList.size();
         dest.writeByte(this.fromType(msg.batchType));
         dest.writeShort(queries);

         for(int i = 0; i < queries; ++i) {
            Object q = msg.queryOrIdList.get(i);
            dest.writeByte((byte)(q instanceof String?0:1));
            if(q instanceof String) {
               CBUtil.writeLongString((String)q, dest);
            } else {
               CBUtil.writeBytes(((MD5Digest)q).bytes, dest);
            }

            CBUtil.writeValueList((List)msg.values.get(i), dest);
         }

         if(version.isSmallerThan(ProtocolVersion.V3)) {
            CBUtil.writeConsistencyLevel(msg.options.getConsistency(), dest);
         } else {
            QueryOptions.codec.encode(msg.options, dest, version);
         }

      }

      public int encodedSize(BatchMessage msg, ProtocolVersion version) {
         int size = 3;

         for(int i = 0; i < msg.queryOrIdList.size(); ++i) {
            Object q = msg.queryOrIdList.get(i);
            size += 1 + (q instanceof String?CBUtil.sizeOfLongString((String)q):CBUtil.sizeOfBytes(((MD5Digest)q).bytes));
            size += CBUtil.sizeOfValueList((List)msg.values.get(i));
         }

         size += version.isSmallerThan(ProtocolVersion.V3)?CBUtil.sizeOfConsistencyLevel(msg.options.getConsistency()):QueryOptions.codec.encodedSize(msg.options, version);
         return size;
      }

      private BatchStatement.Type toType(byte b) {
         if(b == 0) {
            return BatchStatement.Type.LOGGED;
         } else if(b == 1) {
            return BatchStatement.Type.UNLOGGED;
         } else if(b == 2) {
            return BatchStatement.Type.COUNTER;
         } else {
            throw new ProtocolException("Invalid BATCH message type " + b);
         }
      }

      private byte fromType(BatchStatement.Type type) {
         switch (type) {
            case LOGGED: {
               return 0;
            }
            case UNLOGGED: {
               return 1;
            }
            case COUNTER: {
               return 2;
            }
         }
         throw new AssertionError();
      }
   };
   public final BatchStatement.Type batchType;
   public final List<Object> queryOrIdList;
   public final List<List<ByteBuffer>> values;
   public final QueryOptions options;

   public BatchMessage(BatchStatement.Type type, List<Object> queryOrIdList, List<List<ByteBuffer>> values, QueryOptions options) {
      super(Message.Type.BATCH);
      this.batchType = type;
      this.queryOrIdList = queryOrIdList;
      this.values = values;
      this.options = options;
   }

   public Single<? extends Message.Response> execute(Single<QueryState> state, long queryStartNanoTime) {
      try {
         UUID tracingID = this.setUpTracing();
         QueryHandler handler = ClientState.getCQLQueryHandler();
         return state.flatMap((s) -> {
            Single<ResultMessage> resp = Single.defer(() -> {
               this.checkIsLoggedIn(s);
               List<ParsedStatement.Prepared> prepared = new ArrayList(this.queryOrIdList.size());

               for(int ix = 0; ix < this.queryOrIdList.size(); ++ix) {
                  Object query = this.queryOrIdList.get(ix);
                  ParsedStatement.Prepared px;
                  if(query instanceof String) {
                     px = QueryProcessor.parseStatement((String)query, s.cloneWithKeyspaceIfSet(this.options.getKeyspace()));
                  } else {
                     px = handler.getPrepared((MD5Digest)query);
                     if(px == null) {
                        throw new PreparedQueryNotFoundException((MD5Digest)query);
                     }
                  }

                  List<ByteBuffer> queryValues = (List)this.values.get(ix);
                  if(queryValues.size() != px.statement.getBoundTerms()) {
                     throw new InvalidRequestException(String.format("There were %d markers(?) in CQL but %d bound variables", new Object[]{Integer.valueOf(px.statement.getBoundTerms()), Integer.valueOf(queryValues.size())}));
                  }

                  prepared.add(px);
               }

               BatchQueryOptions batchOptions = BatchQueryOptions.withPerStatementVariables(this.options, this.values, this.queryOrIdList);
               List<ModificationStatement> statements = new ArrayList(prepared.size());

               for(int i = 0; i < prepared.size(); ++i) {
                  ParsedStatement.Prepared p = (ParsedStatement.Prepared)prepared.get(i);
                  batchOptions.prepareStatement(i, p.boundNames);
                  if(!(p.statement instanceof ModificationStatement)) {
                     throw new InvalidRequestException("Invalid statement in batch: only UPDATE, INSERT and DELETE statements are allowed.");
                  }

                  statements.add((ModificationStatement)p.statement);
               }

               BatchStatement batch = new BatchStatement(-1, this.batchType, statements, Attributes.none());
               return handler.processBatch(batch, s, batchOptions, this.getCustomPayload(), queryStartNanoTime).map((response) -> {
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
         return Tracing.instance.stopSessionAsync().toSingleDefault(ErrorMessage.fromException(var6));
      }
   }

   private UUID setUpTracing() {
      if(!this.shouldTraceRequest()) {
         return null;
      } else {
         UUID sessionId = Tracing.instance.newSession(this.getCustomPayload());
         Builder<String, String> builder = ImmutableMap.builder();
         if(this.options.getConsistency() != null) {
            builder.put("consistency_level", this.options.getConsistency().name());
         }

         if(this.options.getSerialConsistency() != null) {
            builder.put("serial_consistency_level", this.options.getSerialConsistency().name());
         }

         Tracing.instance.begin("Execute batch of CQL3 queries", this.getClientAddress(), builder.build());
         return sessionId;
      }
   }

   public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("BATCH of [");

      for(int i = 0; i < this.queryOrIdList.size(); ++i) {
         if(i > 0) {
            sb.append(", ");
         }

         sb.append(this.queryOrIdList.get(i)).append(" with ").append(((List)this.values.get(i)).size()).append(" values");
      }

      sb.append("] at consistency ").append(this.options.getConsistency());
      return sb.toString();
   }
}
