package org.apache.cassandra.cql3;

import com.datastax.bdp.db.audit.AuditableEvent;
import com.datastax.bdp.db.audit.IAuditLogger;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Weigher;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.MoreExecutors;
import io.reactivex.Single;
import io.reactivex.SingleSource;
import io.reactivex.functions.Function;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.antlr.runtime.RecognitionException;
import org.apache.cassandra.auth.user.UserRolesAndPermissions;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.CFStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.cql3.statements.SchemaAlteringStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterators;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.exceptions.CassandraException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.metrics.CQLMetrics;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaChangeListener;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MD5Digest;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.flow.RxThreads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryProcessor implements QueryHandler {
   public static final CassandraVersion CQL_VERSION = new CassandraVersion("3.4.5");
   public static final QueryProcessor instance = new QueryProcessor();
   private static final Logger logger = LoggerFactory.getLogger(QueryProcessor.class);
   private static final Cache<MD5Digest, ParsedStatement.Prepared> preparedStatements = Caffeine.newBuilder().executor(MoreExecutors.directExecutor()).maximumWeight(capacityToBytes(DatabaseDescriptor.getPreparedStatementsCacheSizeMB())).weigher(QueryProcessor::measure).removalListener((key, prepared, cause) -> {
      MD5Digest md5Digest = (MD5Digest)key;
      if(cause.wasEvicted()) {
         metrics.preparedStatementsEvicted.inc();
         lastMinuteEvictionsCount.incrementAndGet();
         SystemKeyspace.removePreparedStatement(md5Digest);
      }

   }).build();
   private static final ConcurrentMap<String, ParsedStatement.Prepared> internalStatements = new ConcurrentHashMap();
   public static final CQLMetrics metrics = new CQLMetrics();
   private static final AtomicInteger lastMinuteEvictionsCount = new AtomicInteger(0);
   public static IAuditLogger auditLogger = DatabaseDescriptor.getAuditLogger();

   private static long capacityToBytes(long cacheSizeMB) {
      return cacheSizeMB * 1024L * 1024L;
   }

   public static int preparedStatementsCount() {
      return preparedStatements.asMap().size();
   }

   public static void preloadPreparedStatementBlocking() {
      QueryState state = QueryState.forInternalCalls();
      TPCUtils.blockingAwait(SystemKeyspace.loadPreparedStatements().thenAccept((results) -> {
         int count = 0;
         Iterator var3 = results.iterator();

         while(var3.hasNext()) {
            Pair useKeyspaceAndCQL = (Pair)var3.next();

            try {
               prepare((String)useKeyspaceAndCQL.right, state.cloneWithKeyspaceIfSet((String)useKeyspaceAndCQL.left));
               ++count;
            } catch (RequestValidationException var6) {
               logger.warn("prepared statement recreation error: {}", useKeyspaceAndCQL.right, var6);
            }
         }

         logger.info("Preloaded {} prepared statements", Integer.valueOf(count));
      }));
   }

   @VisibleForTesting
   public static void clearPreparedStatements(boolean memoryOnly) {
      preparedStatements.invalidateAll();
      if(!memoryOnly) {
         SystemKeyspace.resetPreparedStatementsBlocking();
      }

   }

   private static QueryState internalQueryState() {
      return QueryProcessor.InternalStateInstance.INSTANCE.queryState;
   }

   private QueryProcessor() {
      Schema.instance.registerListener(new QueryProcessor.StatementInvalidatingListener());
   }

   public ParsedStatement.Prepared getPrepared(MD5Digest id) {
      return (ParsedStatement.Prepared)preparedStatements.getIfPresent(id);
   }

   public static void validateKey(ByteBuffer key) throws InvalidRequestException {
      if(key != null && key.remaining() != 0) {
         if(key == ByteBufferUtil.UNSET_BYTE_BUFFER) {
            throw new InvalidRequestException("Key may not be unset");
         } else if(key.remaining() > '\uffff') {
            throw new InvalidRequestException("Key length of " + key.remaining() + " is longer than maximum of " + '\uffff');
         }
      } else {
         throw new InvalidRequestException("Key may not be empty");
      }
   }

   public Single<ResultMessage> processStatement(ParsedStatement.Prepared prepared, QueryState queryState, QueryOptions options, long queryStartNanoTime) {
      return this.processStatement(prepared.statement, prepared.rawCQLStatement, prepared.boundNames, queryState, options, queryStartNanoTime);
   }

   public Single<ResultMessage> processStatement(CQLStatement statement, String rawCQLStatement, List<ColumnSpecification> boundNames, QueryState queryState, QueryOptions options, long queryStartNanoTime) {
      if(logger.isTraceEnabled()) {
         logger.trace("Process {} @CL.{}", statement, options.getConsistency());
      }

      List<AuditableEvent> events = auditLogger.getEvents(statement, rawCQLStatement, queryState, options, boundNames);
      StagedScheduler scheduler = statement.getScheduler();
      maybeLogStatementExecution(rawCQLStatement, queryState, statement);

      try {
         this.checkBoundVariables(statement, options);
         Single<ResultMessage> ret = Single.defer(() -> {
            try {
               statement.checkAccess(queryState);
               statement.validate(queryState);
               return auditLogger.logEvents(events).andThen(statement.execute(queryState, options, queryStartNanoTime).onErrorResumeNext(maybeAuditLogErrors(events)));
            } catch (Exception var8) {
               if(!TPCUtils.isWouldBlockException(var8)) {
                  return auditLogger.logFailedQuery(events, var8).andThen(Single.error(var8));
               } else {
                  if(logger.isTraceEnabled()) {
                     logger.trace("Failed to execute blocking operation, retrying on io schedulers");
                  }

                  Single<ResultMessage> single = Single.defer(() -> {
                     statement.checkAccess(queryState);
                     statement.validate(queryState);
                     return auditLogger.logEvents(events).andThen(statement.execute(queryState, options, queryStartNanoTime).onErrorResumeNext(maybeAuditLogErrors(events)));
                  });
                  return RxThreads.subscribeOnIo(single, TPCTaskType.EXECUTE_STATEMENT);
               }
            }
         });
         return scheduler == null?ret:RxThreads.subscribeOn(ret, scheduler, TPCTaskType.EXECUTE_STATEMENT);
      } catch (Exception var11) {
         return auditLogger.logFailedQuery(events, var11).andThen(Single.error(var11));
      }
   }

   public static void maybeLogStatementExecution(String query, QueryState state, CQLStatement statement) {
      if(statement instanceof SchemaAlteringStatement) {
         ClientState cs = state != null?state.getClientState():null;
         logger.info("Executing DDL statement '{}' from IP {} by {}", new Object[]{query, cs != null?(cs.getRemoteAddress() != null?cs.getRemoteAddress():"<internal>"):"<unknown>", cs != null?cs.getUser():"<unknown>"});
      }

   }

   public static Single<ResultMessage> process(String queryString, ConsistencyLevel cl, QueryState queryState, long queryStartNanoTime) throws RequestExecutionException, RequestValidationException {
      return instance.process(queryString, queryState, QueryOptions.forInternalCalls(cl, Collections.emptyList()), queryStartNanoTime);
   }

   public Single<ResultMessage> process(String query, QueryState state, QueryOptions options, Map<String, ByteBuffer> customPayload, long queryStartNanoTime) throws RequestExecutionException, RequestValidationException {
      return this.process(query, state, options, queryStartNanoTime);
   }

   public Single<ResultMessage> process(String queryString, QueryState queryState, QueryOptions options, long queryStartNanoTime) {
      QueryState state = queryState.cloneWithKeyspaceIfSet(options.getKeyspace());

      ParsedStatement.Prepared prepared;
      try {
         prepared = getStatement(queryString, state);
         options.prepare(prepared.boundNames);
      } catch (Exception var9) {
         return auditLogger.logFailedQuery(queryString, state, var9).andThen(Single.error(var9));
      }

      if(!queryState.isSystem()) {
         metrics.regularStatementsExecuted.inc();
      }

      return this.processStatement(prepared, state, options, queryStartNanoTime);
   }

   public static ParsedStatement.Prepared parseStatement(String queryStr, QueryState queryState) throws RequestValidationException {
      try {
         return getStatement(queryStr, queryState);
      } catch (Exception var3) {
         auditLogger.logFailedQuery(queryStr, queryState, var3).subscribe();
         throw var3;
      }
   }

   public static UntypedResultSet processBlocking(String query, ConsistencyLevel cl) throws RequestExecutionException {
      return (UntypedResultSet)TPCUtils.blockingGet(process(query, cl));
   }

   private static Single<UntypedResultSet> process(String query, ConsistencyLevel cl) throws RequestExecutionException {
      return process(query, cl, Collections.emptyList());
   }

   public static Single<UntypedResultSet> process(String query, ConsistencyLevel cl, List<ByteBuffer> values) throws RequestExecutionException {
      Single<? extends ResultMessage> obs = instance.process(query, QueryState.forInternalCalls(), QueryOptions.forInternalCalls(cl, values), System.nanoTime());
      return obs.map((result) -> {
         return result instanceof ResultMessage.Rows?UntypedResultSet.create(((ResultMessage.Rows)result).result):UntypedResultSet.EMPTY;
      });
   }

   private static QueryOptions makeInternalOptions(ParsedStatement.Prepared prepared, Object[] values) {
      return makeInternalOptions(prepared, values, ConsistencyLevel.ONE);
   }

   private static QueryOptions makeInternalOptions(ParsedStatement.Prepared prepared, Object[] values, ConsistencyLevel cl) {
      if(prepared.boundNames.size() != values.length) {
         throw new IllegalArgumentException(String.format("Invalid number of values. Expecting %d but got %d", new Object[]{Integer.valueOf(prepared.boundNames.size()), Integer.valueOf(values.length)}));
      } else {
         List<ByteBuffer> boundValues = createBoundValues(prepared, values);
         return QueryOptions.forInternalCalls(cl, boundValues);
      }
   }

   private static ParsedStatement.Prepared prepareInternal(String query) throws RequestValidationException {
      ParsedStatement.Prepared existing = (ParsedStatement.Prepared)internalStatements.get(query);
      if(existing != null) {
         return existing;
      } else {
         ParsedStatement.Prepared prepared = getStatement(query, internalQueryState());
         prepared.statement.validate(internalQueryState());
         internalStatements.putIfAbsent(query, prepared);
         return prepared;
      }
   }

   public static Single<UntypedResultSet> executeInternalAsync(String query, Object... values) {
      ParsedStatement.Prepared prepared = prepareInternal(query);
      return prepared.statement.executeInternal(internalQueryState(), makeInternalOptions(prepared, values)).map((result) -> {
         return result instanceof ResultMessage.Rows?UntypedResultSet.create(((ResultMessage.Rows)result).result):UntypedResultSet.EMPTY;
      });
   }

   public static UntypedResultSet executeInternal(String query, Object... values) {
      return (UntypedResultSet)TPCUtils.blockingGet(executeInternalAsync(query, values));
   }

   public static UntypedResultSet execute(String query, ConsistencyLevel cl, Object... values) throws RequestExecutionException {
      return execute(query, cl, internalQueryState(), values);
   }

   public static CompletableFuture<UntypedResultSet> executeAsync(String query, ConsistencyLevel cl, Object... values) throws RequestExecutionException {
      return executeAsync(query, cl, internalQueryState(), values);
   }

   public static UntypedResultSet execute(String query, ConsistencyLevel cl, QueryState state, Object... values) throws RequestExecutionException {
      return (UntypedResultSet)TPCUtils.blockingGet(executeAsync(query, cl, state, values));
   }

   public static CompletableFuture<UntypedResultSet> executeAsync(String query, ConsistencyLevel cl, QueryState state, Object... values) throws RequestExecutionException {
      ParsedStatement.Prepared prepared = prepareInternal(query);
      return TPCUtils.toFuture(prepared.statement.execute(state, makeInternalOptions(prepared, values, cl), System.nanoTime())).thenApply((result) -> {
         return result instanceof ResultMessage.Rows?UntypedResultSet.create(((ResultMessage.Rows)result).result):UntypedResultSet.EMPTY;
      });
   }

   public static UntypedResultSet executeInternalWithPaging(String query, PageSize pageSize, Object... values) {
      ParsedStatement.Prepared prepared = prepareInternal(query);
      if(!(prepared.statement instanceof SelectStatement)) {
         throw new IllegalArgumentException("Only SELECTs can be paged");
      } else {
         SelectStatement select = (SelectStatement)prepared.statement;
         QueryPager pager = select.getQuery(QueryState.forInternalCalls(), makeInternalOptions(prepared, values), FBUtilities.nowInSeconds()).getPager((PagingState)null, ProtocolVersion.CURRENT);
         return UntypedResultSet.create(select, pager, pageSize);
      }
   }

   private static List<ByteBuffer> createBoundValues(ParsedStatement.Prepared prepared, Object... values) {
      List<ByteBuffer> boundValues = new ArrayList(values.length);

      for(int i = 0; i < values.length; ++i) {
         Object value = values[i];
         AbstractType type = ((ColumnSpecification)prepared.boundNames.get(i)).type;
         boundValues.add(!(value instanceof ByteBuffer) && value != null?type.decompose(value):(ByteBuffer)value);
      }

      return boundValues;
   }

   public static Single<UntypedResultSet> executeOnceInternal(String query, Object... values) {
      ParsedStatement.Prepared prepared = getStatement(query, internalQueryState());
      StagedScheduler scheduler = prepared.statement.getScheduler();
      Single<? extends ResultMessage> observable = Single.defer(() -> {
         prepared.statement.validate(internalQueryState());
         return prepared.statement.executeInternal(internalQueryState(), makeInternalOptions(prepared, values));
      });
      if(scheduler != null) {
         observable = RxThreads.subscribeOn(observable, scheduler, TPCTaskType.EXECUTE_STATEMENT);
      }

      return observable.map((result) -> {
         return result instanceof ResultMessage.Rows?UntypedResultSet.create(((ResultMessage.Rows)result).result):UntypedResultSet.EMPTY;
      });
   }

   public static Single<UntypedResultSet> executeInternalWithNow(int nowInSec, long queryStartNanoTime, String query, Object... values) {
      ParsedStatement.Prepared prepared = prepareInternal(query);

      assert prepared.statement instanceof SelectStatement;

      SelectStatement select = (SelectStatement)prepared.statement;
      return select.executeInternal(internalQueryState(), makeInternalOptions(prepared, values), nowInSec, queryStartNanoTime).map((result) -> {
         return UntypedResultSet.create(result.result);
      });
   }

   public static UntypedResultSet resultify(String query, RowIterator partition) {
      return resultify(query, PartitionIterators.singletonIterator(partition));
   }

   public static UntypedResultSet resultify(String query, PartitionIterator partitions) {
      SelectStatement ss = (SelectStatement)getStatement(query, QueryState.forInternalCalls()).statement;
      ResultSet cqlRows = ss.process(partitions, FBUtilities.nowInSeconds());
      return UntypedResultSet.create(cqlRows);
   }

   public Single<ResultMessage.Prepared> prepare(String query, QueryState queryState, Map<String, ByteBuffer> customPayload) throws RequestValidationException {
      return prepare(query, queryState);
   }

   public static Single<ResultMessage.Prepared> prepare(String queryString, QueryState state) {
      return prepare(queryString, state, true);
   }

   public static Single<ResultMessage.Prepared> prepare(String queryString, QueryState state, boolean storeStatementOnDisk) {
      List events = Collections.emptyList();

      try {
         String rawKeyspace = state.getClientState().getRawKeyspace();
         ResultMessage.Prepared existing = getStoredPreparedStatement(queryString, rawKeyspace);
         if(existing != null) {
            return Single.just(existing);
         } else {
            ParsedStatement.Prepared prepared = getStatement(queryString, state);
            validateBindingMarkers(prepared);
            events = auditLogger.getEventsForPrepare(prepared.statement, queryString, state);
            Single<ResultMessage.Prepared> single = storePreparedStatement(queryString, rawKeyspace, prepared, storeStatementOnDisk);
            return auditLogger.logEvents(events).andThen(single.onErrorResumeNext(maybeAuditLogErrors(events)));
         }
      } catch (Exception var8) {
         return events.isEmpty()?auditLogger.logFailedQuery(queryString, state, var8).andThen(Single.error(var8)):auditLogger.logFailedQuery(events, var8).andThen(Single.error(var8));
      }
   }

   public static void validateBindingMarkers(ParsedStatement.Prepared prepared) {
      int boundTerms = prepared.statement.getBoundTerms();
      if(boundTerms > '\uffff') {
         throw new InvalidRequestException(String.format("Too many markers(?). %d markers exceed the allowed maximum of %d", new Object[]{Integer.valueOf(boundTerms), Integer.valueOf('\uffff')}));
      } else {
         assert boundTerms == prepared.boundNames.size();

      }
   }

   private static MD5Digest computeId(String queryString, String keyspace) {
      String toHash = keyspace == null?queryString:keyspace + queryString;
      return MD5Digest.compute(toHash);
   }

   public static ResultMessage.Prepared getStoredPreparedStatement(String queryString, String keyspace) throws InvalidRequestException {
      MD5Digest statementId = computeId(queryString, keyspace);
      ParsedStatement.Prepared existing = (ParsedStatement.Prepared)preparedStatements.getIfPresent(statementId);
      if(existing == null) {
         return null;
      } else {
         RequestValidations.checkTrue(queryString.equals(existing.rawCQLStatement), String.format("MD5 hash collision: query with the same MD5 hash was already prepared. \n Existing: '%s'", new Object[]{existing.rawCQLStatement}));
         return new ResultMessage.Prepared(statementId, existing.resultMetadataId, existing);
      }
   }

   public static Single<ResultMessage.Prepared> storePreparedStatement(String queryString, String keyspace, ParsedStatement.Prepared prepared, boolean storeStatementOnDisk) {
      long statementSize = ObjectSizes.measureDeep(prepared.statement);
      if(statementSize > capacityToBytes(DatabaseDescriptor.getPreparedStatementsCacheSizeMB())) {
         throw new InvalidRequestException(String.format("Prepared statement of size %d bytes is larger than allowed maximum of %d MB: %s...", new Object[]{Long.valueOf(statementSize), Long.valueOf(DatabaseDescriptor.getPreparedStatementsCacheSizeMB()), queryString.substring(0, 200)}));
      } else {
         MD5Digest statementId = computeId(queryString, keyspace);
         preparedStatements.put(statementId, prepared);
         ResultSet.ResultMetadata resultMetadata = ResultSet.ResultMetadata.fromPrepared(prepared);
         if(!storeStatementOnDisk) {
            return Single.just(new ResultMessage.Prepared(statementId, resultMetadata.getResultMetadataId(), prepared));
         } else {
            Single<UntypedResultSet> observable = SystemKeyspace.writePreparedStatement(keyspace, statementId, queryString);
            return observable.map((resultSet) -> {
               return new ResultMessage.Prepared(statementId, resultMetadata.getResultMetadataId(), prepared);
            });
         }
      }
   }

   public Single<ResultMessage> processPrepared(ParsedStatement.Prepared prepared, QueryState state, QueryOptions options, Map<String, ByteBuffer> customPayload, long queryStartNanoTime) {
      return this.processPrepared(prepared, state, options, queryStartNanoTime);
   }

   public Single<ResultMessage> processPrepared(ParsedStatement.Prepared prepared, QueryState queryState, QueryOptions options, long queryStartNanoTime) {
      if(!queryState.isSystem()) {
         metrics.preparedStatementsExecuted.inc();
      }

      return this.processStatement(prepared, queryState, options, queryStartNanoTime);
   }

   private void checkBoundVariables(CQLStatement statement, QueryOptions options) {
      List<ByteBuffer> variables = options.getValues();
      if(!variables.isEmpty() || statement.getBoundTerms() != 0) {
         RequestValidations.checkFalse(variables.size() != statement.getBoundTerms(), "there were %d markers(?) in CQL but %d bound variables", Integer.valueOf(statement.getBoundTerms()), Integer.valueOf(variables.size()));
         if(logger.isTraceEnabled()) {
            for(int i = 0; i < variables.size(); ++i) {
               logger.trace("[{}] '{}'", Integer.valueOf(i + 1), variables.get(i));
            }
         }
      }

   }

   public Single<ResultMessage> processBatch(BatchStatement statement, QueryState state, BatchQueryOptions options, Map<String, ByteBuffer> customPayload, long queryStartNanoTime) {
      return this.processBatch(statement, state, options, queryStartNanoTime);
   }

   public Single<ResultMessage> processBatch(BatchStatement batch, QueryState queryState, BatchQueryOptions options, long queryStartNanoTime) {
      QueryState state = queryState.cloneWithKeyspaceIfSet(options.getKeyspace());
      return Single.defer(() -> {
         List events = auditLogger.getEvents(batch, queryState, options);

         try {
            batch.checkAccess(state);
            batch.validate();
            batch.validate(state);
            return auditLogger.logEvents(events).andThen(batch.execute(state, options, queryStartNanoTime).onErrorResumeNext(maybeAuditLogErrors(events)));
         } catch (RuntimeException var8) {
            return !TPCUtils.isWouldBlockException(var8)?auditLogger.logFailedQuery(events, var8).andThen(Single.error(var8)):RxThreads.subscribeOnIo(Single.defer(() -> {
               batch.checkAccess(state);
               batch.validate();
               batch.validate(state);
               return auditLogger.logEvents(events).andThen(batch.execute(state, options, queryStartNanoTime).onErrorResumeNext(maybeAuditLogErrors(events)));
            }), TPCTaskType.EXECUTE_STATEMENT);
         }
      });
   }

   public static ParsedStatement.Prepared getStatement(String queryStr, QueryState state) throws RequestValidationException {
      Tracing.trace("Parsing {}", (Object)queryStr);
      ParsedStatement statement = parseStatement(queryStr);
      if(statement instanceof CFStatement) {
         CFStatement cfStatement = (CFStatement)statement;
         cfStatement.prepareKeyspace(state.getClientState());
      }

      Tracing.trace("Preparing statement");
      ParsedStatement.Prepared prepared = statement.prepare();
      prepared.rawCQLStatement = queryStr;
      return prepared;
   }

   public static <T extends ParsedStatement> T parseStatement(String queryStr, Class<T> klass, String type) throws SyntaxException {
      try {
         ParsedStatement stmt = parseStatement(queryStr);
         if(!klass.isAssignableFrom(stmt.getClass())) {
            throw new IllegalArgumentException("Invalid query, must be a " + type + " statement but was: " + stmt.getClass());
         } else {
            return (ParsedStatement)klass.cast(stmt);
         }
      } catch (RequestValidationException var4) {
         throw new IllegalArgumentException(var4.getMessage(), var4);
      }
   }

   public static ParsedStatement parseStatement(String queryStr) throws SyntaxException {
      try {
         return (ParsedStatement)CQLFragmentParser.parseAnyUnhandled(CqlParser::query, queryStr);
      } catch (CassandraException var2) {
         throw var2;
      } catch (RuntimeException var3) {
         logger.error(String.format("The statement: [%s] could not be parsed.", new Object[]{queryStr}), var3);
         throw new SyntaxException(String.format("Failed parsing statement: [%s] reason: %s %s", new Object[]{queryStr, var3.getClass().getSimpleName(), var3.getMessage()}));
      } catch (RecognitionException var4) {
         throw new SyntaxException("Invalid or malformed CQL query string: " + var4.getMessage());
      }
   }

   private static int measure(Object key, ParsedStatement.Prepared value) {
      return Ints.checkedCast(ObjectSizes.measureDeep(key) + ObjectSizes.measureDeep(value));
   }

   @VisibleForTesting
   public static void clearInternalStatementsCache() {
      internalStatements.clear();
   }

   public static <T extends ResultMessage> Function<Throwable, SingleSource<T>> maybeAuditLogErrors(List<AuditableEvent> events) {
      return events.isEmpty()?Single::error:(e) -> {
         return auditLogger.logFailedQuery(events, e).andThen(Single.error(e));
      };
   }

   static {
      ScheduledExecutors.scheduledTasks.scheduleAtFixedRate(() -> {
         long count = (long)lastMinuteEvictionsCount.getAndSet(0);
         if(count > 0L) {
            logger.warn("{} prepared statements discarded in the last minute because cache limit reached ({} MB)", Long.valueOf(count), Long.valueOf(DatabaseDescriptor.getPreparedStatementsCacheSizeMB()));
         }

      }, 1L, 1L, TimeUnit.MINUTES);
      logger.info("Initialized prepared statement caches with {} MB", Long.valueOf(DatabaseDescriptor.getPreparedStatementsCacheSizeMB()));
   }

   private static class StatementInvalidatingListener extends SchemaChangeListener {
      private StatementInvalidatingListener() {
      }

      private static void removeInvalidPreparedStatements(String ksName, String cfName) {
         removeInvalidPreparedStatements(QueryProcessor.internalStatements.values().iterator(), ksName, cfName);
         removeInvalidPersistentPreparedStatements(QueryProcessor.preparedStatements.asMap().entrySet().iterator(), ksName, cfName);
      }

      private static void removePreparedStatementBlocking(MD5Digest key) {
         TPCUtils.blockingAwait(SystemKeyspace.removePreparedStatement(key));
      }

      private static void removeInvalidPreparedStatementsForFunction(String ksName, String functionName) {
         Predicate<org.apache.cassandra.cql3.functions.Function> matchesFunction = (f) -> {
            return ksName.equals(f.name().keyspace) && functionName.equals(f.name().name);
         };
         Iterator iter = QueryProcessor.preparedStatements.asMap().entrySet().iterator();

         while(iter.hasNext()) {
            Entry<MD5Digest, ParsedStatement.Prepared> pstmt = (Entry)iter.next();
            Iterable var10000 = ((ParsedStatement.Prepared)pstmt.getValue()).statement.getFunctions();
            matchesFunction.getClass();
            if(Iterables.any(var10000, matchesFunction::test)) {
               removePreparedStatementBlocking((MD5Digest)pstmt.getKey());
               iter.remove();
            }
         }

         Iterators.removeIf(QueryProcessor.internalStatements.values().iterator(), (statement) -> {
            Iterable var10000 = statement.statement.getFunctions();
            matchesFunction.getClass();
            return Iterables.any(var10000, matchesFunction::test);
         });
      }

      private static void removeInvalidPersistentPreparedStatements(Iterator<Entry<MD5Digest, ParsedStatement.Prepared>> iterator, String ksName, String cfName) {
         while(iterator.hasNext()) {
            Entry<MD5Digest, ParsedStatement.Prepared> entry = (Entry)iterator.next();
            if(shouldInvalidate(ksName, cfName, ((ParsedStatement.Prepared)entry.getValue()).statement)) {
               removePreparedStatementBlocking((MD5Digest)entry.getKey());
               iterator.remove();
            }
         }

      }

      private static void removeInvalidPreparedStatements(Iterator<ParsedStatement.Prepared> iterator, String ksName, String cfName) {
         while(iterator.hasNext()) {
            if(shouldInvalidate(ksName, cfName, ((ParsedStatement.Prepared)iterator.next()).statement)) {
               iterator.remove();
            }
         }

      }

      private static boolean shouldInvalidate(String ksName, String cfName, CQLStatement statement) {
         String statementKsName;
         String statementCfName;
         if(statement instanceof ModificationStatement) {
            ModificationStatement modificationStatement = (ModificationStatement)statement;
            statementKsName = modificationStatement.keyspace();
            statementCfName = modificationStatement.columnFamily();
         } else {
            if(!(statement instanceof SelectStatement)) {
               if(statement instanceof BatchStatement) {
                  BatchStatement batchStatement = (BatchStatement)statement;
                  Iterator var6 = batchStatement.getStatements().iterator();

                  ModificationStatement stmt;
                  do {
                     if(!var6.hasNext()) {
                        return false;
                     }

                     stmt = (ModificationStatement)var6.next();
                  } while(!shouldInvalidate(ksName, cfName, stmt));

                  return true;
               }

               return false;
            }

            SelectStatement selectStatement = (SelectStatement)statement;
            statementKsName = selectStatement.keyspace();
            statementCfName = selectStatement.columnFamily();
         }

         return ksName.equals(statementKsName) && (cfName == null || cfName.equals(statementCfName));
      }

      public void onCreateFunction(String ksName, String functionName, List<AbstractType<?>> argTypes) {
         onCreateFunctionInternal(ksName, functionName, argTypes);
      }

      public void onCreateAggregate(String ksName, String aggregateName, List<AbstractType<?>> argTypes) {
         onCreateFunctionInternal(ksName, aggregateName, argTypes);
      }

      private static void onCreateFunctionInternal(String ksName, String functionName, List<AbstractType<?>> argTypes) {
         if(Schema.instance.getKeyspaceMetadata(ksName).functions.get(new FunctionName(ksName, functionName)).size() > 1) {
            removeInvalidPreparedStatementsForFunction(ksName, functionName);
         }

      }

      public void onAlterTable(String ksName, String cfName, boolean affectsStatements) {
         QueryProcessor.logger.trace("Column definitions for {}.{} changed, invalidating related prepared statements", ksName, cfName);
         if(affectsStatements) {
            removeInvalidPreparedStatements(ksName, cfName);
         }

      }

      public void onAlterFunction(String ksName, String functionName, List<AbstractType<?>> argTypes) {
         removeInvalidPreparedStatementsForFunction(ksName, functionName);
      }

      public void onAlterAggregate(String ksName, String aggregateName, List<AbstractType<?>> argTypes) {
         removeInvalidPreparedStatementsForFunction(ksName, aggregateName);
      }

      public void onDropKeyspace(String ksName) {
         QueryProcessor.logger.trace("Keyspace {} was dropped, invalidating related prepared statements", ksName);
         removeInvalidPreparedStatements(ksName, (String)null);
      }

      public void onDropTable(String ksName, String cfName) {
         QueryProcessor.logger.trace("Table {}.{} was dropped, invalidating related prepared statements", ksName, cfName);
         removeInvalidPreparedStatements(ksName, cfName);
      }

      public void onDropFunction(String ksName, String functionName, List<AbstractType<?>> argTypes) {
         removeInvalidPreparedStatementsForFunction(ksName, functionName);
      }

      public void onDropAggregate(String ksName, String aggregateName, List<AbstractType<?>> argTypes) {
         removeInvalidPreparedStatementsForFunction(ksName, aggregateName);
      }
   }

   private static enum InternalStateInstance {
      INSTANCE;

      private final QueryState queryState;

      private InternalStateInstance() {
         ClientState state = ClientState.forInternalCalls();
         state.setKeyspace("system");
         this.queryState = new QueryState(state, UserRolesAndPermissions.SYSTEM);
      }
   }
}
