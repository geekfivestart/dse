package com.datastax.bdp.cassandra.cql3;

import com.datastax.bdp.cassandra.auth.RowLevelAccessControlAuthorizer;
import com.datastax.bdp.db.audit.AuditableEvent;
import io.reactivex.Single;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement.Prepared;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

public class DseStandardQueryOperationFactory implements DseQueryOperationFactory {
   public DseStandardQueryOperationFactory() {
   }

   public DseStandardQueryOperationFactory.StandardOperation create(String cql, QueryState state, QueryOptions options, Map<String, ByteBuffer> customPayload, long queryStartNanoTime, boolean auditStatement) {
      return new DseStandardQueryOperationFactory.StandardOperation(cql, state, options, customPayload, queryStartNanoTime, auditStatement);
   }

   public DseStandardQueryOperationFactory.PreparedOperation createPrepared(Prepared statement, QueryState state, QueryOptions options, Map<String, ByteBuffer> customPayload, long queryStartNanoTime, boolean auditStatement) {
      return new DseStandardQueryOperationFactory.PreparedOperation(statement, state, options, customPayload, queryStartNanoTime, auditStatement);
   }

   public DseStandardQueryOperationFactory.BatchOperation createBatch(BatchStatement statement, QueryState state, BatchQueryOptions options, Map<String, ByteBuffer> customPayload, long queryStartNanoTime, boolean auditStatement) {
      return new DseStandardQueryOperationFactory.BatchOperation(statement, state, options, customPayload, queryStartNanoTime, auditStatement);
   }

   public class BatchOperation extends BaseQueryOperation<BatchStatement, BatchQueryOptions> {
      BatchOperation(BatchStatement this$0, QueryState statement, BatchQueryOptions queryState, Map<String, ByteBuffer> options, long customPayload, boolean queryStartNanoTime) {
         super((String)null, statement, queryState.cloneWithKeyspaceIfSet(options.getKeyspace()), options, (List)null, customPayload, queryStartNanoTime, auditStatement);
      }

      List<AuditableEvent> getEventsFromAuditLogger() {
         return Collections.emptyList();
      }

      void approveRlac() {
         RowLevelAccessControlAuthorizer.approveBatchStatement((BatchStatement)this.statement, this.queryState, (BatchQueryOptions)this.options);
      }

      Single<ResultMessage> execute() {
         return QueryProcessor.instance.processBatch((BatchStatement)this.statement, this.queryState, (BatchQueryOptions)this.options, this.queryStartNanoTime);
      }

      boolean auditable() {
         return true;
      }
   }

   public class PreparedOperation extends BaseQueryOperation<CQLStatement, QueryOptions> {
      PreparedOperation(Prepared this$0, QueryState prepared, QueryOptions queryState, Map<String, ByteBuffer> options, long customPayload, boolean queryStartNanoTime) {
         super(prepared.rawCQLStatement, prepared.statement, queryState.cloneWithKeyspaceIfSet(options.getKeyspace()), options, prepared.boundNames, customPayload, queryStartNanoTime, auditStatement);
      }

      void approveRlac() {
         this.statement = RowLevelAccessControlAuthorizer.approveStatement(this.statement, this.queryState, (QueryOptions)this.options);
      }

      List<AuditableEvent> getEventsFromAuditLogger() {
         return Collections.emptyList();
      }

      Single<ResultMessage> execute() {
         if(!this.queryState.isSystem()) {
            QueryProcessor.metrics.preparedStatementsExecuted.inc();
         }

         return QueryProcessor.instance.processStatement(this.statement, this.cql, this.boundNames, this.queryState, (QueryOptions)this.options, this.queryStartNanoTime);
      }

      boolean auditable() {
         return this.options == null || ((QueryOptions)this.options).getPagingOptions() == null || ((QueryOptions)this.options).getPagingOptions().state() == null;
      }
   }

   public class StandardOperation extends BaseQueryOperation<CQLStatement, QueryOptions> {
      StandardOperation(String this$0, QueryState cql, QueryOptions queryState, Map<String, ByteBuffer> options, long customPayload, boolean queryStartNanoTime) {
         super(cql, (CQLStatement)null, queryState.cloneWithKeyspaceIfSet(options.getKeyspace()), options, (List)null, customPayload, queryStartNanoTime, auditStatement);
      }

      void parse() {
         Prepared prepared = QueryProcessor.getStatement(this.cql, this.queryState);
         this.statement = prepared.statement;
         this.boundNames = prepared.boundNames;
      }

      void approveRlac() {
         this.statement = RowLevelAccessControlAuthorizer.approveStatement(this.statement, this.queryState, (QueryOptions)this.options);
      }

      boolean auditable() {
         return this.options == null || ((QueryOptions)this.options).getPagingOptions() == null || ((QueryOptions)this.options).getPagingOptions().state() == null;
      }

      List<AuditableEvent> getEventsFromAuditLogger() {
         return Collections.emptyList();
      }

      Single<ResultMessage> execute() {
         ((QueryOptions)this.options).prepare(this.boundNames);
         if(!this.queryState.isSystem()) {
            QueryProcessor.metrics.regularStatementsExecuted.inc();
         }

         return QueryProcessor.instance.processStatement(this.statement, this.cql, this.boundNames, this.queryState, (QueryOptions)this.options, this.queryStartNanoTime);
      }
   }
}
