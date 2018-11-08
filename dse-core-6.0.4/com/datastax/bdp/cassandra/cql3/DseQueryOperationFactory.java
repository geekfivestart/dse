package com.datastax.bdp.cassandra.cql3;

import io.reactivex.Single;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement.Prepared;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

public interface DseQueryOperationFactory {
   default DseQueryOperationFactory.Operation<CQLStatement, QueryOptions> create(String cql, QueryState state, QueryOptions options, Map<String, ByteBuffer> customPayload, long queryStartNanoTime, boolean auditStatement) {
      return null;
   }

   default DseQueryOperationFactory.Operation<CQLStatement, QueryOptions> createPrepared(Prepared statement, QueryState state, QueryOptions options, Map<String, ByteBuffer> customPayload, long queryStartNanoTime, boolean auditStatement) {
      return null;
   }

   default DseQueryOperationFactory.Operation<BatchStatement, BatchQueryOptions> createBatch(BatchStatement statement, QueryState state, BatchQueryOptions options, Map<String, ByteBuffer> customPayload, long queryStartNanoTime, boolean auditStatement) {
      return null;
   }

   default ParsedStatement maybeInjectCustomRestrictions(ParsedStatement statement, Map<String, ByteBuffer> customPayload) {
      return statement;
   }

   public interface Operation<S extends CQLStatement, O> {
      S getStatement();

      QueryState getQueryState();

      O getOptions();

      boolean isInternal();

      String getTableName();

      String getCql();

      Single<ResultMessage> process();
   }
}
