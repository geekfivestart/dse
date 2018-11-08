package org.apache.cassandra.cql3.statements;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import io.reactivex.Completable;
import io.reactivex.CompletableSource;
import io.reactivex.Single;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.metrics.BatchMetrics;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.flow.RxThreads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

public class BatchStatement implements CQLStatement {
   private final int boundTerms;
   public final BatchStatement.Type type;
   private final List<ModificationStatement> statements;
   private final Map<TableId, RegularAndStaticColumns> updatedColumns;
   private final RegularAndStaticColumns conditionColumns;
   private final boolean updatesRegularRows;
   private final boolean updatesStaticRow;
   private final Attributes attrs;
   private final boolean hasConditions;
   private static final Logger logger = LoggerFactory.getLogger(BatchStatement.class);
   private static final String UNLOGGED_BATCH_WARNING = "Unlogged batch covering {} partitions detected against table{} {}. You should use a logged batch for atomicity, or asynchronous writes for performance.";
   private static final String LOGGED_BATCH_LOW_GCGS_WARNING = "Executing a LOGGED BATCH on table{} {}, configured with a gc_grace_seconds of 0. The gc_grace_seconds is used to TTL batchlog entries, so setting gc_grace_seconds too low on tables involved in an atomic batch might cause batchlog entries to expire before being replayed.";
   public static final BatchMetrics metrics = new BatchMetrics();

   public BatchStatement(int boundTerms, BatchStatement.Type type, List<ModificationStatement> statements, Attributes attrs) {
      this.boundTerms = boundTerms;
      this.type = type;
      this.statements = statements;
      this.attrs = attrs;
      boolean hasConditions = false;
      BatchStatement.MultiTableColumnsBuilder regularBuilder = new BatchStatement.MultiTableColumnsBuilder();
      RegularAndStaticColumns.Builder conditionBuilder = RegularAndStaticColumns.builder();
      boolean updateRegular = false;
      boolean updateStatic = false;
      Iterator var10 = statements.iterator();

      while(var10.hasNext()) {
         ModificationStatement stmt = (ModificationStatement)var10.next();
         regularBuilder.addAll(stmt.metadata(), stmt.updatedColumns());
         updateRegular |= stmt.updatesRegularRows();
         if(stmt.hasConditions()) {
            hasConditions = true;
            conditionBuilder.addAll(stmt.conditionColumns());
            updateStatic |= stmt.updatesStaticRow();
         }
      }

      this.updatedColumns = regularBuilder.build();
      this.conditionColumns = conditionBuilder.build();
      this.updatesRegularRows = updateRegular;
      this.updatesStaticRow = updateStatic;
      this.hasConditions = hasConditions;
   }

   public AuditableEventType getAuditEventType() {
      return CoreAuditableEventType.BATCH;
   }

   public Iterable<Function> getFunctions() {
      List<Function> functions = new ArrayList();
      Iterator var2 = this.statements.iterator();

      while(var2.hasNext()) {
         ModificationStatement statement = (ModificationStatement)var2.next();
         statement.addFunctionsTo(functions);
      }

      return functions;
   }

   public int getBoundTerms() {
      return this.boundTerms;
   }

   public void checkAccess(QueryState state) {
      Iterator var2 = this.statements.iterator();

      while(var2.hasNext()) {
         ModificationStatement statement = (ModificationStatement)var2.next();
         statement.checkAccess(state);
      }

   }

   public void validate() throws InvalidRequestException {
      if(this.attrs.isTimeToLiveSet()) {
         throw new InvalidRequestException("Global TTL on the BATCH statement is not supported.");
      } else {
         boolean timestampSet = this.attrs.isTimestampSet();
         if(timestampSet) {
            if(this.hasConditions) {
               throw new InvalidRequestException("Cannot provide custom timestamp for conditional BATCH");
            }

            if(this.isCounter()) {
               throw new InvalidRequestException("Cannot provide custom timestamp for counter BATCH");
            }
         }

         boolean hasCounters = false;
         boolean hasNonCounters = false;
         Iterator var4 = this.statements.iterator();

         while(var4.hasNext()) {
            ModificationStatement statement = (ModificationStatement)var4.next();
            if(timestampSet && statement.isCounter()) {
               throw new InvalidRequestException("Cannot provide custom timestamp for a BATCH containing counters");
            }

            if(timestampSet && statement.isTimestampSet()) {
               throw new InvalidRequestException("Timestamp must be set either on BATCH or individual statements");
            }

            if(this.isCounter() && !statement.isCounter()) {
               throw new InvalidRequestException("Cannot include non-counter statement in a counter batch");
            }

            if(this.isLogged() && statement.isCounter()) {
               throw new InvalidRequestException("Cannot include a counter statement in a logged batch");
            }

            if(statement.isCounter()) {
               hasCounters = true;
            } else {
               hasNonCounters = true;
            }
         }

         if(hasCounters && hasNonCounters) {
            throw new InvalidRequestException("Counter and non-counter mutations cannot exist in the same batch");
         } else {
            if(this.hasConditions) {
               String ksName = null;
               String cfName = null;

               ModificationStatement stmt;
               for(Iterator var6 = this.statements.iterator(); var6.hasNext(); cfName = stmt.columnFamily()) {
                  stmt = (ModificationStatement)var6.next();
                  if(ksName != null && (!stmt.keyspace().equals(ksName) || !stmt.columnFamily().equals(cfName))) {
                     throw new InvalidRequestException("Batch with conditions cannot span multiple tables");
                  }

                  ksName = stmt.keyspace();
               }
            }

         }
      }
   }

   public StagedScheduler getScheduler() {
      return null;
   }

   private boolean isCounter() {
      return this.type == BatchStatement.Type.COUNTER;
   }

   private boolean isLogged() {
      return this.type == BatchStatement.Type.LOGGED;
   }

   public void validate(QueryState state) throws InvalidRequestException {
      Iterator var2 = this.statements.iterator();

      while(var2.hasNext()) {
         ModificationStatement statement = (ModificationStatement)var2.next();
         statement.validate(state);
      }

   }

   public List<ModificationStatement> getStatements() {
      return this.statements;
   }

   private Single<Collection<? extends IMutation>> getMutations(BatchQueryOptions options, boolean local, long now, long queryStartNanoTime) throws RequestExecutionException, RequestValidationException {
      Set<String> tablesWithZeroGcGs = new HashSet();
      UpdatesCollector collector = new UpdatesCollector(this.updatedColumns, this.updatedRows());
      List<Completable> completables = new ArrayList(this.statements.size());

      for(int i = 0; i < this.statements.size(); ++i) {
         ModificationStatement statement = (ModificationStatement)this.statements.get(i);
         if(this.isLogged() && statement.metadata().params.gcGraceSeconds == 0) {
            tablesWithZeroGcGs.add(statement.metadata.toString());
         }

         QueryOptions statementOptions = options.forStatement(i);
         long timestamp = this.attrs.getTimestamp(now, statementOptions);
         completables.add(statement.addUpdates(collector, statementOptions, local, timestamp, queryStartNanoTime));
      }

      return Completable.concat(completables).andThen(Single.fromCallable(() -> {
         if(!tablesWithZeroGcGs.isEmpty()) {
            String suffix = tablesWithZeroGcGs.size() == 1?"":"s";
            NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1L, TimeUnit.MINUTES, "Executing a LOGGED BATCH on table{} {}, configured with a gc_grace_seconds of 0. The gc_grace_seconds is used to TTL batchlog entries, so setting gc_grace_seconds too low on tables involved in an atomic batch might cause batchlog entries to expire before being replayed.", new Object[]{suffix, tablesWithZeroGcGs});
            ClientWarn.instance.warn(MessageFormatter.arrayFormat("Executing a LOGGED BATCH on table{} {}, configured with a gc_grace_seconds of 0. The gc_grace_seconds is used to TTL batchlog entries, so setting gc_grace_seconds too low on tables involved in an atomic batch might cause batchlog entries to expire before being replayed.", new Object[]{suffix, tablesWithZeroGcGs}).getMessage());
         }

         collector.validateIndexedColumns();
         return collector.toMutations();
      }));
   }

   private int updatedRows() {
      return this.statements.size();
   }

   private static void verifyBatchSize(Collection<? extends IMutation> mutations) throws InvalidRequestException {
      if(mutations.size() > 1) {
         long warnThreshold = (long)DatabaseDescriptor.getBatchSizeWarnThreshold();
         long size = IMutation.dataSize(mutations);
         if(size > warnThreshold) {
            Set<String> tableNames = new HashSet();
            Iterator var6 = mutations.iterator();

            while(var6.hasNext()) {
               IMutation mutation = (IMutation)var6.next();
               Iterator var8 = mutation.getPartitionUpdates().iterator();

               while(var8.hasNext()) {
                  PartitionUpdate update = (PartitionUpdate)var8.next();
                  tableNames.add(update.metadata().toString());
               }
            }

            long failThreshold = DatabaseDescriptor.getBatchSizeFailThreshold();
            String format = "Batch for {} is of size {}, exceeding specified threshold of {} by {}.{}";
            if(size > failThreshold) {
               Tracing.trace(format, new Object[]{tableNames, FBUtilities.prettyPrintMemory(size), FBUtilities.prettyPrintMemory(failThreshold), FBUtilities.prettyPrintMemory(size - failThreshold), " (see batch_size_fail_threshold_in_kb)"});
               logger.error(format, new Object[]{tableNames, FBUtilities.prettyPrintMemory(size), FBUtilities.prettyPrintMemory(failThreshold), FBUtilities.prettyPrintMemory(size - failThreshold), " (see batch_size_fail_threshold_in_kb)"});
               throw new InvalidRequestException("Batch too large");
            }

            if(logger.isWarnEnabled()) {
               logger.warn(format, new Object[]{tableNames, FBUtilities.prettyPrintMemory(size), FBUtilities.prettyPrintMemory(warnThreshold), FBUtilities.prettyPrintMemory(size - warnThreshold), ""});
            }

            ClientWarn.instance.warn(MessageFormatter.arrayFormat(format, new Object[]{tableNames, Long.valueOf(size), Long.valueOf(warnThreshold), Long.valueOf(size - warnThreshold), ""}).getMessage());
         }

      }
   }

   private void verifyBatchType(Collection<? extends IMutation> mutations) {
      if(!this.isLogged() && mutations.size() > 1) {
         Set<DecoratedKey> keySet = new HashSet();
         Set<String> tableNames = new HashSet();
         Iterator var4 = mutations.iterator();

         while(var4.hasNext()) {
            IMutation mutation = (IMutation)var4.next();
            Iterator var6 = mutation.getPartitionUpdates().iterator();

            while(var6.hasNext()) {
               PartitionUpdate update = (PartitionUpdate)var6.next();
               keySet.add(update.partitionKey());
               tableNames.add(update.metadata().toString());
            }
         }

         if(keySet.size() > DatabaseDescriptor.getUnloggedBatchAcrossPartitionsWarnThreshold()) {
            NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1L, TimeUnit.MINUTES, "Unlogged batch covering {} partitions detected against table{} {}. You should use a logged batch for atomicity, or asynchronous writes for performance.", new Object[]{Integer.valueOf(keySet.size()), tableNames.size() == 1?"":"s", tableNames});
            ClientWarn.instance.warn(MessageFormatter.arrayFormat("Unlogged batch covering {} partitions detected against table{} {}. You should use a logged batch for atomicity, or asynchronous writes for performance.", new Object[]{Integer.valueOf(keySet.size()), tableNames.size() == 1?"":"s", tableNames}).getMessage());
         }
      }

   }

   public Single<ResultMessage> execute(QueryState queryState, QueryOptions options, long queryStartNanoTime) {
      return this.execute(queryState, BatchQueryOptions.withoutPerStatementVariables(options), queryStartNanoTime);
   }

   public Single<ResultMessage> execute(QueryState queryState, BatchQueryOptions options, long queryStartNanoTime) {
      return this.execute(queryState, options, options.getTimestamp(queryState), queryStartNanoTime);
   }

   private Single<ResultMessage> execute(QueryState queryState, BatchQueryOptions options, long now, long queryStartNanoTime) {
      return options.getConsistency() == null?Single.error(new InvalidRequestException("Invalid empty consistency level")):(options.getSerialConsistency() == null?Single.error(new InvalidRequestException("Invalid empty serial consistency level")):(!this.hasConditions?this.executeWithoutConditions(this.getMutations(options, false, now, queryStartNanoTime), options.getConsistency(), queryStartNanoTime):this.executeWithConditions(options, queryState, queryStartNanoTime)));
   }

   private Single<ResultMessage> executeWithoutConditions(Single<Collection<? extends IMutation>> mutationsSingle, ConsistencyLevel cl, long queryStartNanoTime) {
      return mutationsSingle.flatMap((mutations) -> {
         if(mutations.isEmpty()) {
            return Single.just(new ResultMessage.Void());
         } else {
            try {
               verifyBatchSize(mutations);
               this.verifyBatchType(mutations);
            } catch (InvalidRequestException var6) {
               return Single.error(var6);
            }

            this.updatePartitionsPerBatchMetrics(mutations.size());
            boolean mutateAtomic = this.isLogged() && mutations.size() > 1;
            return StorageProxy.mutateWithTriggers(mutations, cl, mutateAtomic, queryStartNanoTime);
         }
      });
   }

   private void updatePartitionsPerBatchMetrics(int updatedPartitions) {
      if(this.isLogged()) {
         metrics.partitionsPerLoggedBatch.update(updatedPartitions);
      } else if(this.isCounter()) {
         metrics.partitionsPerCounterBatch.update(updatedPartitions);
      } else {
         metrics.partitionsPerUnloggedBatch.update(updatedPartitions);
      }

   }

   private Single<ResultMessage> executeWithConditions(BatchQueryOptions options, QueryState state, long queryStartNanoTime) throws RequestExecutionException, RequestValidationException {
      Pair<CQL3CasRequest, Set<ColumnMetadata>> p = this.makeCasRequest(options, state);
      CQL3CasRequest casRequest = (CQL3CasRequest)p.left;
      Set<ColumnMetadata> columnsWithConditions = (Set)p.right;
      String ksName = casRequest.metadata.keyspace;
      String tableName = casRequest.metadata.name;
      Single<ResultMessage> result = Single.defer(() -> {
         return ModificationStatement.buildCasResultSet(ksName, tableName, StorageProxy.cas(ksName, tableName, casRequest.key, casRequest, options.getSerialConsistency(), options.getConsistency(), state.getClientState(), queryStartNanoTime), columnsWithConditions, true, options.forStatement(0));
      }).map(ResultMessage.Rows::<init>);
      return RxThreads.subscribeOnIo(result, TPCTaskType.CAS);
   }

   private Pair<CQL3CasRequest, Set<ColumnMetadata>> makeCasRequest(BatchQueryOptions options, QueryState state) {
      long now = state.getTimestamp();
      DecoratedKey key = null;
      CQL3CasRequest casRequest = null;
      Set<ColumnMetadata> columnsWithConditions = new LinkedHashSet();

      for(int i = 0; i < this.statements.size(); ++i) {
         ModificationStatement statement = (ModificationStatement)this.statements.get(i);
         QueryOptions statementOptions = options.forStatement(i);
         long timestamp = this.attrs.getTimestamp(now, statementOptions);
         List<ByteBuffer> pks = statement.buildPartitionKeyNames(statementOptions);
         if(statement.getRestrictions().keyIsInRelation()) {
            throw new IllegalArgumentException("Batch with conditions cannot span multiple partitions (you cannot use IN on the partition key)");
         }

         if(key == null) {
            key = statement.metadata().partitioner.decorateKey((ByteBuffer)pks.get(0));
            casRequest = new CQL3CasRequest(statement.metadata(), key, true, this.conditionColumns, this.updatesRegularRows, this.updatesStaticRow);
         } else if(!key.getKey().equals(pks.get(0))) {
            throw new InvalidRequestException("Batch with conditions cannot span multiple partitions");
         }

         RequestValidations.checkFalse(statement.getRestrictions().clusteringKeyRestrictionsHasIN(), "IN on the clustering key columns is not supported with conditional %s", statement.type.isUpdate()?"updates":"deletions");
         if(statement.hasSlices()) {
            assert !statement.hasConditions();

            Slices slices = statement.createSlices(statementOptions);
            if(!slices.isEmpty()) {
               Iterator var15 = slices.iterator();

               while(var15.hasNext()) {
                  Slice slice = (Slice)var15.next();
                  casRequest.addRangeDeletion(slice, statement, statementOptions, timestamp);
               }
            }
         } else {
            Clustering clustering = (Clustering)Iterables.getOnlyElement(statement.createClustering(statementOptions));
            if(statement.hasConditions()) {
               statement.addConditions(clustering, casRequest, statementOptions);
               if(!statement.hasIfNotExistCondition() && !statement.hasIfExistCondition()) {
                  if(columnsWithConditions != null) {
                     Iterables.addAll(columnsWithConditions, statement.getColumnsWithConditions());
                  }
               } else {
                  columnsWithConditions = null;
               }
            }

            casRequest.addRowUpdate(clustering, statement, statementOptions, timestamp);
         }
      }

      return Pair.create(casRequest, columnsWithConditions);
   }

   public boolean hasConditions() {
      return this.hasConditions;
   }

   public Single<ResultMessage> executeInternal(QueryState queryState, QueryOptions options) throws RequestValidationException, RequestExecutionException {
      return this.hasConditions?this.executeInternalWithConditions(BatchQueryOptions.withoutPerStatementVariables(options), queryState):this.executeInternalWithoutCondition(queryState, options, System.nanoTime());
   }

   private Single<ResultMessage> executeInternalWithoutCondition(QueryState queryState, QueryOptions options, long queryStartNanoTime) throws RequestValidationException, RequestExecutionException {
      return this.getMutations(BatchQueryOptions.withoutPerStatementVariables(options), true, queryState.getTimestamp(), queryStartNanoTime).flatMapCompletable((mutations) -> {
         if(mutations.isEmpty()) {
            return Completable.complete();
         } else {
            List<Completable> mutationObservables = new ArrayList(mutations.size());
            Iterator var2 = mutations.iterator();

            while(var2.hasNext()) {
               IMutation mutation = (IMutation)var2.next();
               mutationObservables.add(mutation.applyAsync());
            }

            return (CompletableSource)(mutationObservables.size() == 1?(CompletableSource)mutationObservables.get(0):Completable.concat(mutationObservables));
         }
      }).andThen(Single.just(new ResultMessage.Void()));
   }

   private Single<ResultMessage> executeInternalWithConditions(BatchQueryOptions options, QueryState state) throws RequestExecutionException, RequestValidationException {
      Pair<CQL3CasRequest, Set<ColumnMetadata>> p = this.makeCasRequest(options, state);
      CQL3CasRequest request = (CQL3CasRequest)p.left;
      Set<ColumnMetadata> columnsWithConditions = (Set)p.right;
      String ksName = request.metadata.keyspace;
      String tableName = request.metadata.name;
      return ModificationStatement.casInternal(request, state).flatMap((result) -> {
         return ModificationStatement.buildCasResultSet(ksName, tableName, result, columnsWithConditions, true, options.forStatement(0));
      }).map(ResultMessage.Rows::<init>);
   }

   public String toString() {
      return String.format("BatchStatement(type=%s, statements=%s)", new Object[]{this.type, this.statements});
   }

   private static class MultiTableColumnsBuilder {
      private final Map<TableId, RegularAndStaticColumns.Builder> perTableBuilders;

      private MultiTableColumnsBuilder() {
         this.perTableBuilders = new HashMap();
      }

      public void addAll(TableMetadata table, RegularAndStaticColumns columns) {
         RegularAndStaticColumns.Builder builder = (RegularAndStaticColumns.Builder)this.perTableBuilders.computeIfAbsent(table.id, (k) -> {
            return RegularAndStaticColumns.builder();
         });
         builder.addAll(columns);
      }

      public Map<TableId, RegularAndStaticColumns> build() {
         Map<TableId, RegularAndStaticColumns> m = Maps.newHashMapWithExpectedSize(this.perTableBuilders.size());
         Iterator var2 = this.perTableBuilders.entrySet().iterator();

         while(var2.hasNext()) {
            Entry<TableId, RegularAndStaticColumns.Builder> p = (Entry)var2.next();
            m.put(p.getKey(), ((RegularAndStaticColumns.Builder)p.getValue()).build());
         }

         return m;
      }
   }

   public static class Parsed extends CFStatement {
      private final BatchStatement.Type type;
      private final Attributes.Raw attrs;
      private final List<ModificationStatement.Parsed> parsedStatements;

      public Parsed(BatchStatement.Type type, Attributes.Raw attrs, List<ModificationStatement.Parsed> parsedStatements) {
         super((CFName)null);
         this.type = type;
         this.attrs = attrs;
         this.parsedStatements = parsedStatements;
      }

      public void prepareKeyspace(ClientState state) throws InvalidRequestException {
         Iterator var2 = this.parsedStatements.iterator();

         while(var2.hasNext()) {
            ModificationStatement.Parsed statement = (ModificationStatement.Parsed)var2.next();
            statement.prepareKeyspace(state);
         }

      }

      public ParsedStatement.Prepared prepare() throws InvalidRequestException {
         VariableSpecifications boundNames = this.getBoundVariables();
         String firstKS = null;
         String firstCF = null;
         boolean haveMultipleCFs = false;
         List<ModificationStatement> statements = new ArrayList(this.parsedStatements.size());

         ModificationStatement.Parsed parsed;
         for(Iterator var6 = this.parsedStatements.iterator(); var6.hasNext(); statements.add(parsed.prepare(boundNames))) {
            parsed = (ModificationStatement.Parsed)var6.next();
            if(firstKS == null) {
               firstKS = parsed.keyspace();
               firstCF = parsed.columnFamily();
            } else if(!haveMultipleCFs) {
               haveMultipleCFs = !firstKS.equals(parsed.keyspace()) || !firstCF.equals(parsed.columnFamily());
            }
         }

         Attributes prepAttrs = this.attrs.prepare("[batch]", "[batch]");
         prepAttrs.collectMarkerSpecification(boundNames);
         BatchStatement batchStatement = new BatchStatement(boundNames.size(), this.type, statements, prepAttrs);
         batchStatement.validate();
         short[] partitionKeyBindIndexes = !haveMultipleCFs && !batchStatement.statements.isEmpty()?boundNames.getPartitionKeyBindIndexes(((ModificationStatement)batchStatement.statements.get(0)).metadata()):null;
         return new ParsedStatement.Prepared(batchStatement, boundNames, partitionKeyBindIndexes);
      }
   }

   public static enum Type {
      LOGGED,
      UNLOGGED,
      COUNTER;

      private Type() {
      }
   }
}
