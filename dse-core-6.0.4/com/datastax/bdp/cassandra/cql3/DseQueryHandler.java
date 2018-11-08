package com.datastax.bdp.cassandra.cql3;

import com.datastax.bdp.cassandra.metrics.UserObjectLatencyPlugin;
import com.datastax.bdp.db.audit.IAuditLogger;
import com.datastax.bdp.graph.DseGraphQueryOperationFactory;
import com.google.inject.Inject;
import io.reactivex.Completable;
import io.reactivex.Single;
import io.reactivex.functions.Function;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement.Prepared;
import org.apache.cassandra.db.filter.RowFilter.UserExpression;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.MD5Digest;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DseQueryHandler implements QueryHandler {
   private static final Logger logger;
   public static final String PROXY_EXECUTE = "ProxyExecute";
   private static final DseQueryHandler instance;
   @Inject
   public static UserObjectLatencyPlugin latencyPlugin;
   @Inject
   public static CqlSlowLogPlugin slowLogPlugin;
   private static IAuditLogger auditLogger;
   @Inject(
      optional = true
   )
   private static DseQueryOperationFactory operationFactory;
   @Inject(
      optional = true
   )
   private static DseGraphQueryOperationFactory graphOperationFactory;

   public DseQueryHandler() {
   }

   public static DseQueryHandler getInstance() {
      return instance;
   }

   public static DseGraphQueryOperationFactory getGraphQueryOperationFactory() {
      return graphOperationFactory;
   }

   public Single<ResultMessage> process(String query, QueryState state, QueryOptions options, Map<String, ByteBuffer> customPayload, long queryStartNanoTime) throws RequestExecutionException, RequestValidationException {
      return this.process(query, state, options, customPayload, queryStartNanoTime, true);
   }

   public Single<ResultMessage> process(String query, QueryState state, QueryOptions options, Map<String, ByteBuffer> customPayload, long queryStartNanoTime, boolean auditStatement) throws RequestExecutionException, RequestValidationException {
      DseQueryOperationFactory factory = operationFactory;
      if(customPayload != null && customPayload.containsKey("graph-language")) {
         if(null == graphOperationFactory) {
            throw new InvalidRequestException("DSE Graph not configured to process queries");
         }

         factory = graphOperationFactory;
      }

      return ((DseQueryOperationFactory)factory).create(query, state, options, customPayload, queryStartNanoTime, auditStatement).process();
   }

   public Single<ResultMessage> processPrepared(Prepared statement, QueryState state, QueryOptions options, Map<String, ByteBuffer> customPayload, long queryStartNanoTime) throws RequestExecutionException, RequestValidationException {
      return this.processPrepared(statement, state, options, customPayload, queryStartNanoTime, true);
   }

   public Single<ResultMessage> processPrepared(Prepared statement, QueryState state, QueryOptions options, Map<String, ByteBuffer> customPayload, long queryStartNanoTime, boolean auditStatement) throws RequestExecutionException, RequestValidationException {
      return operationFactory.createPrepared(statement, state, options, customPayload, queryStartNanoTime, auditStatement).process();
   }

   public Single<ResultMessage> processBatch(BatchStatement statement, QueryState state, BatchQueryOptions options, Map<String, ByteBuffer> customPayload, long queryStartNanoTime) throws RequestExecutionException, RequestValidationException {
      return this.processBatch(statement, state, options, customPayload, queryStartNanoTime, true);
   }

   public Single<ResultMessage> processBatch(BatchStatement statement, QueryState state, BatchQueryOptions options, Map<String, ByteBuffer> customPayload, long queryStartNanoTime, boolean auditStatement) throws RequestExecutionException, RequestValidationException {
      return operationFactory.createBatch(statement, state, options, customPayload, queryStartNanoTime, auditStatement).process();
   }

   public Single<org.apache.cassandra.transport.messages.ResultMessage.Prepared> prepare(String query, QueryState state, Map<String, ByteBuffer> customPayload) throws RequestValidationException {
      return this.prepareInternal(query, state, customPayload).map((p) -> {
         return (org.apache.cassandra.transport.messages.ResultMessage.Prepared)p.right;
      });
   }

   public Single<Pair<CQLStatement, org.apache.cassandra.transport.messages.ResultMessage.Prepared>> prepareInternal(String query, QueryState state, Map<String, ByteBuffer> customPayload) throws RequestValidationException {
      return this.prepareInternal(query, state, customPayload, true).map((pair) -> {
         return Pair.create(((Prepared)pair.left).statement, pair.right);
      });
   }

   public Single<Pair<Prepared, org.apache.cassandra.transport.messages.ResultMessage.Prepared>> prepareInternal(String query, QueryState state, Map<String, ByteBuffer> customPayload, boolean auditStatement) throws RequestValidationException {
      try {
         Single<Pair<Prepared, org.apache.cassandra.transport.messages.ResultMessage.Prepared>> prepared = StatementUtils.prepareStatementMaybeCustom(query, operationFactory, state, customPayload);
         prepared = prepared.onErrorResumeNext((e) -> {
            return this.auditLogFailedQuery(query, state, e).andThen(Single.error(e));
         });
         prepared = prepared.flatMap((pair) -> {
            CQLStatement statement = ((Prepared)pair.left).statement;
            return auditStatement?auditLogger.logEvents(auditLogger.getEventsForPrepare(statement, query, state)).andThen(Single.just(pair)):Single.just(pair);
         });
         return prepared;
      } catch (Exception var6) {
         return this.auditLogFailedQuery(query, state, var6).andThen(Single.error(var6));
      }
   }

   private Completable auditLogFailedQuery(String query, QueryState state, Throwable e) {
      logger.trace("Unexpected exception when trying to prepare a statement for internal use: " + e.getMessage(), e);
      return auditLogger.logFailedQuery(query, state, e);
   }

   public Prepared getPrepared(MD5Digest id) {
      return QueryProcessor.instance.getPrepared(id);
   }

   static {
      UserExpression.register(RLACExpression.class, new RLACExpression.Deserializer());
      logger = LoggerFactory.getLogger(DseQueryHandler.class);
      instance = new DseQueryHandler();
      auditLogger = DatabaseDescriptor.getAuditLogger();
      operationFactory = new DseStandardQueryOperationFactory();
   }
}
