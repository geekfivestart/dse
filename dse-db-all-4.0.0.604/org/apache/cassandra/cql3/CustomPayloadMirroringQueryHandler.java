package org.apache.cassandra.cql3;

import io.reactivex.Single;
import io.reactivex.functions.Function;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.MD5Digest;

public class CustomPayloadMirroringQueryHandler implements QueryHandler {
   static QueryProcessor queryProcessor;

   public CustomPayloadMirroringQueryHandler() {
   }

   public Single<ResultMessage> process(String query, QueryState state, QueryOptions options, Map<String, ByteBuffer> customPayload, long queryStartNanoTime) {
      return queryProcessor.process(query, state, options, customPayload, queryStartNanoTime).map((result) -> {
         result.setCustomPayload(customPayload);
         return result;
      });
   }

   public Single<ResultMessage.Prepared> prepare(String query, QueryState queryState, Map<String, ByteBuffer> customPayload) {
      Single<ResultMessage.Prepared> observable = queryProcessor.prepare(query, queryState, customPayload);
      return observable.map((prepared) -> {
         prepared.setCustomPayload(customPayload);
         return prepared;
      });
   }

   public ParsedStatement.Prepared getPrepared(MD5Digest id) {
      return queryProcessor.getPrepared(id);
   }

   public Single<ResultMessage> processPrepared(ParsedStatement.Prepared prepared, QueryState state, QueryOptions options, Map<String, ByteBuffer> customPayload, long queryStartNanoTime) {
      return queryProcessor.processPrepared(prepared, state, options, customPayload, queryStartNanoTime).map((result) -> {
         result.setCustomPayload(customPayload);
         return result;
      });
   }

   public Single<ResultMessage> processBatch(BatchStatement statement, QueryState state, BatchQueryOptions options, Map<String, ByteBuffer> customPayload, long queryStartNanoTime) {
      return queryProcessor.processBatch(statement, state, options, customPayload, queryStartNanoTime).map((result) -> {
         result.setCustomPayload(customPayload);
         return result;
      });
   }

   static {
      queryProcessor = QueryProcessor.instance;
   }
}
