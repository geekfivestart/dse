package com.datastax.bdp.util;

import com.datastax.bdp.cassandra.cql3.StatementUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.ParsedStatement.Prepared;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.transport.messages.ResultMessage.Rows;
import org.apache.cassandra.utils.MD5Digest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraUtil {
   public static final String CASSANDRA_RETRIES_SLEEP = "cassandra.retries.sleep";
   public static final String CASSANDRA_RETRIES_PER_CL = "cassandra.retries.per.cl";
   private static final Logger logger = LoggerFactory.getLogger(CassandraUtil.class);
   private static final Cache<String, MD5Digest> preparedStatements = CacheBuilder.newBuilder().softValues().build();
   private static final int retryAttemptSleep = Integer.valueOf(System.getProperty("cassandra.retries.sleep", "100")).intValue();
   private static final int retriesPerCl;

   private CassandraUtil() {
   }

   public static void robustInsert(List<Mutation> mutations, ConsistencyLevel... consistencyLevels) {
      Exception error = null;
      int attempt = 0;

      while(attempt < totalRetries(consistencyLevels)) {
         try {
            TPCUtils.blockingGet(StorageProxy.mutateWithTriggers(mutations, getConsistencyLevel(attempt, consistencyLevels), true, System.nanoTime()));
            return;
         } catch (Exception var6) {
            logger.warn("Error {} on attempt {} out of {} with CL {}...", new Object[]{var6.getMessage(), Integer.valueOf(attempt + 1), Integer.valueOf(totalRetries(consistencyLevels)), getConsistencyLevel(attempt, consistencyLevels)});
            error = var6;

            try {
               Thread.sleep((long)retryAttemptSleep);
            } catch (InterruptedException var5) {
               ;
            }

            ++attempt;
         }
      }

      throw new RuntimeException("Insert command failed after " + totalRetries(consistencyLevels) + " attempts, source exception follows.", error);
   }

   public static UntypedResultSet robustCql3Statement(ClientState cs, String cql, ConsistencyLevel... consistencyLevels) {
      Exception error = null;
      int attempt = 0;

      while(attempt < totalRetries(consistencyLevels)) {
         try {
            QueryState qs = StatementUtils.queryStateBlocking(cs);
            ResultMessage result = (ResultMessage)TPCUtils.blockingGet(QueryProcessor.process(cql, getConsistencyLevel(attempt, consistencyLevels), qs, System.nanoTime()));
            if(result instanceof Rows) {
               return UntypedResultSet.create(((Rows)result).result);
            }

            return UntypedResultSet.EMPTY;
         } catch (Exception var8) {
            logger.warn("Error {} on attempt {} out of {} with CL {}...", new Object[]{var8.getMessage(), Integer.valueOf(attempt + 1), Integer.valueOf(totalRetries(consistencyLevels)), getConsistencyLevel(attempt, consistencyLevels)});
            error = var8;

            try {
               Thread.sleep((long)retryAttemptSleep);
            } catch (InterruptedException var7) {
               ;
            }

            ++attempt;
         }
      }

      throw new RuntimeException("CQL3 statement failed after " + totalRetries(consistencyLevels) + " attempts, source exception follows.", error);
   }

   public static ResultMessage robustCql3PreparedStatement(QueryState queryState, UnboundStatement statement, ConsistencyLevel... consistencyLevels) {
      Exception error = null;
      int attempt = 0;

      while(attempt < totalRetries(consistencyLevels)) {
         try {
            QueryHandler queryHandler = ClientState.getCQLQueryHandler();
            Prepared parsedPrepared = prepareStatement(statement.cql, queryHandler, queryState);
            return (ResultMessage)TPCUtils.blockingGet(parsedPrepared.statement.execute(queryState, QueryOptions.forInternalCalls(getConsistencyLevel(attempt, consistencyLevels), statement.variables), System.nanoTime()));
         } catch (Exception var8) {
            logger.warn("Error {} on attempt {} out of {} with CL {}...", new Object[]{var8.getMessage(), Integer.valueOf(attempt + 1), Integer.valueOf(totalRetries(consistencyLevels)), getConsistencyLevel(attempt, consistencyLevels)});
            error = var8;

            try {
               Thread.sleep((long)retryAttemptSleep);
            } catch (InterruptedException var7) {
               ;
            }

            ++attempt;
         }
      }

      throw new RuntimeException("CQL3 prepared statement failed after " + totalRetries(consistencyLevels) + " attempts, source exception follows.", error);
   }

   public static void internalCql3PreparedStatement(String cql, Iterator<List<ByteBuffer>> variables, Iterator<CassandraUtil.EventHandler<ResultMessage>> handlers) {
      try {
         QueryState queryState = QueryState.forInternalCalls();
         QueryHandler queryHandler = ClientState.getCQLQueryHandler();
         Prepared prepared = prepareStatement(cql, queryHandler, queryState);

         while(variables.hasNext()) {
            List<ByteBuffer> bindings = (List)variables.next();
            CassandraUtil.EventHandler<ResultMessage> handler = (CassandraUtil.EventHandler)handlers.next();
            handler.preExecute((Object)null);
            ResultMessage result = (ResultMessage)TPCUtils.blockingGet(prepared.statement.executeInternal(queryState, QueryOptions.forInternalCalls(bindings)));
            handler.postExecute(result);
         }

      } catch (ExecutionException var9) {
         Throwable cause = var9.getCause() != null?var9.getCause():var9;
         throw new RuntimeException((Throwable)cause);
      }
   }

   private static int totalRetries(ConsistencyLevel... consistencyLevels) {
      return consistencyLevels.length * retriesPerCl;
   }

   private static ConsistencyLevel getConsistencyLevel(int attempt, ConsistencyLevel... consistencyLevels) {
      return consistencyLevels[attempt / retriesPerCl];
   }

   private static Prepared prepareStatement(String cql, QueryHandler queryHandler, QueryState queryState) throws ExecutionException, RequestValidationException {
      MD5Digest statementId;
      Prepared prepared;
      for(statementId = (MD5Digest)preparedStatements.get(cql, () -> {
         return ((org.apache.cassandra.transport.messages.ResultMessage.Prepared)TPCUtils.blockingGet(queryHandler.prepare(cql, queryState, (Map)null))).statementId;
      }); (prepared = queryHandler.getPrepared(statementId)) == null; statementId = ((org.apache.cassandra.transport.messages.ResultMessage.Prepared)TPCUtils.blockingGet(queryHandler.prepare(cql, queryState, (Map)null))).statementId) {
         ;
      }

      preparedStatements.put(cql, statementId);
      return prepared;
   }

   @VisibleForTesting
   public static long getPreparedStatementsCacheSize() {
      return preparedStatements.size();
   }

   static {
      Integer configuredRetries = Integer.valueOf(System.getProperty("cassandra.retries.per.cl", "2"));
      retriesPerCl = configuredRetries.intValue() > 0?configuredRetries.intValue():1;
   }

   public interface EventHandler<P> {
      void preExecute(P var1);

      void postExecute(P var1);
   }
}
