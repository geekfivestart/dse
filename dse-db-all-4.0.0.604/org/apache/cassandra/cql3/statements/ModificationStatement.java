package org.apache.cassandra.cql3.statements;

import com.google.common.collect.Iterables;
import io.reactivex.Completable;
import io.reactivex.CompletableSource;
import io.reactivex.Single;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.Operation;
import org.apache.cassandra.cql3.Operations;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.UpdateParameters;
import org.apache.cassandra.cql3.Validation;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.WhereClause;
import org.apache.cassandra.cql3.conditions.ColumnCondition;
import org.apache.cassandra.cql3.conditions.ColumnConditions;
import org.apache.cassandra.cql3.conditions.Conditions;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.db.CBuilder;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.ReadContext;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.db.rows.FlowablePartitions;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.triggers.TriggerExecutor;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MD5Digest;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.flow.RxThreads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ModificationStatement implements CQLStatement, TableStatement {
   protected static final Logger logger = LoggerFactory.getLogger(ModificationStatement.class);
   private static final MD5Digest EMPTY_HASH = MD5Digest.wrap(new byte[0]);
   public static final String CUSTOM_EXPRESSIONS_NOT_ALLOWED = "Custom index expressions cannot be used in WHERE clauses for UPDATE or DELETE statements";
   private static final ColumnIdentifier CAS_RESULT_COLUMN = new ColumnIdentifier("[applied]", false);
   protected final StatementType type;
   private final int boundTerms;
   public final TableMetadata metadata;
   private final Attributes attrs;
   private final StatementRestrictions restrictions;
   private final Operations operations;
   private final RegularAndStaticColumns updatedColumns;
   private final Conditions conditions;
   private final RegularAndStaticColumns conditionColumns;
   private final RegularAndStaticColumns requiredReadColumns;
   private final boolean requiresRead;

   public ModificationStatement(StatementType type, int boundTerms, TableMetadata metadata, Operations operations, StatementRestrictions restrictions, Conditions conditions, Attributes attrs) {
      this.type = type;
      this.boundTerms = boundTerms;
      this.metadata = metadata;
      this.restrictions = restrictions;
      this.operations = operations;
      this.conditions = conditions;
      this.attrs = attrs;
      if(!conditions.isEmpty()) {
         RequestValidations.checkFalse(metadata.isCounter(), "Conditional updates are not supported on counter tables");
         RequestValidations.checkFalse(attrs.isTimestampSet(), "Cannot provide custom timestamp for conditional updates");
      }

      RegularAndStaticColumns.Builder conditionColumnsBuilder = RegularAndStaticColumns.builder();
      Iterable<ColumnMetadata> columns = conditions.getColumns();
      if(columns != null) {
         conditionColumnsBuilder.addAll(columns);
      }

      boolean requiresRead = false;
      RegularAndStaticColumns.Builder updatedColumnsBuilder = RegularAndStaticColumns.builder();
      RegularAndStaticColumns.Builder requiresReadBuilder = RegularAndStaticColumns.builder();
      Iterator var13 = operations.iterator();

      while(var13.hasNext()) {
         Operation operation = (Operation)var13.next();
         updatedColumnsBuilder.add(operation.column);
         if(operation.requiresRead()) {
            conditionColumnsBuilder.add(operation.column);
            requiresReadBuilder.add(operation.column);
            requiresRead = true;
         }
      }

      this.requiresRead = requiresRead;
      RegularAndStaticColumns modifiedColumns = updatedColumnsBuilder.build();
      if(metadata.isCompactTable() && modifiedColumns.isEmpty() && this.updatesRegularRows()) {
         modifiedColumns = metadata.regularAndStaticColumns();
      }

      this.updatedColumns = modifiedColumns;
      this.conditionColumns = conditionColumnsBuilder.build();
      this.requiredReadColumns = requiresReadBuilder.build();
   }

   public Iterable<Function> getFunctions() {
      List<Function> functions = new ArrayList();
      this.addFunctionsTo(functions);
      return functions;
   }

   public void addFunctionsTo(List<Function> functions) {
      this.attrs.addFunctionsTo(functions);
      this.restrictions.addFunctionsTo(functions);
      this.operations.addFunctionsTo(functions);
      this.conditions.addFunctionsTo(functions);
   }

   public TableMetadata metadata() {
      return this.metadata;
   }

   public StatementRestrictions getRestrictions() {
      return this.restrictions;
   }

   public abstract void addUpdateForKey(PartitionUpdate var1, Clustering var2, UpdateParameters var3);

   public abstract void addUpdateForKey(PartitionUpdate var1, Slice var2, UpdateParameters var3);

   public int getBoundTerms() {
      return this.boundTerms;
   }

   public String keyspace() {
      return this.metadata.keyspace;
   }

   public String columnFamily() {
      return this.metadata.name;
   }

   public boolean isCounter() {
      return this.metadata().isCounter();
   }

   public boolean isView() {
      return this.metadata().isView();
   }

   public long getTimestamp(long now, QueryOptions options) throws InvalidRequestException {
      return this.attrs.getTimestamp(now, options);
   }

   public boolean isTimestampSet() {
      return this.attrs.isTimestampSet();
   }

   public int getTimeToLive(QueryOptions options) throws InvalidRequestException {
      return this.attrs.getTimeToLive(options, this.metadata);
   }

   public void checkAccess(QueryState state) {
      state.checkTablePermission((TableMetadata)this.metadata, CorePermission.MODIFY);
      if(this.hasConditions()) {
         state.checkTablePermission((TableMetadata)this.metadata, CorePermission.SELECT);
      }

      Iterator var2 = this.getFunctions().iterator();

      while(var2.hasNext()) {
         Function function = (Function)var2.next();
         state.checkFunctionPermission((Function)function, CorePermission.EXECUTE);
      }

   }

   public void validate(QueryState state) throws InvalidRequestException {
      RequestValidations.checkFalse(this.hasConditions() && this.attrs.isTimestampSet(), "Cannot provide custom timestamp for conditional updates");
      RequestValidations.checkFalse(this.isCounter() && this.attrs.isTimestampSet(), "Cannot provide custom timestamp for counter updates");
      RequestValidations.checkFalse(this.isCounter() && this.attrs.isTimeToLiveSet(), "Cannot provide custom TTL for counter updates");
      RequestValidations.checkFalse(this.isView(), "Cannot directly modify a materialized view");
   }

   public StagedScheduler getScheduler() {
      return null;
   }

   public RegularAndStaticColumns updatedColumns() {
      return this.updatedColumns;
   }

   public RegularAndStaticColumns conditionColumns() {
      return this.conditionColumns;
   }

   public boolean updatesRegularRows() {
      return this.metadata().clusteringColumns().isEmpty() || this.restrictions.hasClusteringColumnsRestrictions();
   }

   public boolean updatesStaticRow() {
      return this.operations.appliesToStaticColumns();
   }

   public List<Operation> getRegularOperations() {
      return this.operations.regularOperations();
   }

   public List<Operation> getStaticOperations() {
      return this.operations.staticOperations();
   }

   public Iterable<Operation> allOperations() {
      return this.operations;
   }

   public Iterable<ColumnMetadata> getColumnsWithConditions() {
      return this.conditions.getColumns();
   }

   public boolean hasIfNotExistCondition() {
      return this.conditions.isIfNotExists();
   }

   public boolean hasIfExistCondition() {
      return this.conditions.isIfExists();
   }

   public List<ByteBuffer> buildPartitionKeyNames(QueryOptions options) throws InvalidRequestException {
      List<ByteBuffer> partitionKeys = this.restrictions.getPartitionKeys(options);
      Iterator var3 = partitionKeys.iterator();

      while(var3.hasNext()) {
         ByteBuffer key = (ByteBuffer)var3.next();
         QueryProcessor.validateKey(key);
      }

      return partitionKeys;
   }

   public NavigableSet<Clustering> createClustering(QueryOptions options) throws InvalidRequestException {
      return this.appliesOnlyToStaticColumns() && !this.restrictions.hasClusteringColumnsRestrictions()?FBUtilities.singleton(CBuilder.STATIC_BUILDER.build(), this.metadata().comparator):this.restrictions.getClusteringColumns(options);
   }

   private boolean appliesOnlyToStaticColumns() {
      return appliesOnlyToStaticColumns(this.operations, this.conditions);
   }

   public static boolean appliesOnlyToStaticColumns(Operations operation, Conditions conditions) {
      return !operation.appliesToRegularColumns() && !conditions.appliesToRegularColumns() && (operation.appliesToStaticColumns() || conditions.appliesToStaticColumns());
   }

   public boolean requiresRead() {
      return this.requiresRead;
   }

   private Single<Map<DecoratedKey, Partition>> readRequiredLists(Collection<ByteBuffer> partitionKeys, ClusteringIndexFilter filter, DataLimits limits, boolean local, ConsistencyLevel cl, long queryStartNanoTime) {
      if(!this.requiresRead) {
         return Single.just(Collections.emptyMap());
      } else {
         try {
            cl.validateForRead(this.keyspace());
         } catch (InvalidRequestException var12) {
            throw new InvalidRequestException(String.format("Write operation require a read but consistency %s is not supported on reads", new Object[]{cl}));
         }

         List<SinglePartitionReadCommand> commands = new ArrayList(partitionKeys.size());
         int nowInSec = FBUtilities.nowInSeconds();
         Iterator var10 = partitionKeys.iterator();

         while(var10.hasNext()) {
            ByteBuffer key = (ByteBuffer)var10.next();
            commands.add(SinglePartitionReadCommand.create(this.metadata(), nowInSec, ColumnFilter.selection(this.requiredReadColumns), RowFilter.NONE, limits, this.metadata().partitioner.decorateKey(key), filter));
         }

         SinglePartitionReadCommand.Group group = new SinglePartitionReadCommand.Group(commands, DataLimits.NONE);
         Flow<FlowablePartition> partitions = local?group.executeInternal():group.execute(ReadContext.builder(group, cl).build(queryStartNanoTime));
         return partitions.flatMap((p) -> {
            return FilteredPartition.create(p);
         }).reduceToRxSingle(new HashMap(), (map, partition) -> {
            map.put(partition.partitionKey(), partition);
            return map;
         });
      }
   }

   public boolean hasConditions() {
      return !this.conditions.isEmpty();
   }

   public boolean hasSlices() {
      return this.type.allowClusteringColumnSlices() && this.getRestrictions().hasClusteringColumnsRestrictions() && this.getRestrictions().isColumnRange();
   }

   public Single<ResultMessage> execute(QueryState queryState, QueryOptions options, long queryStartNanoTime) {
      return options.getConsistency() == null?Single.error(new InvalidRequestException("Invalid empty consistency level")):(!this.hasConditions()?this.executeWithoutCondition(queryState, options, queryStartNanoTime):this.executeWithCondition(queryState, options, queryStartNanoTime));
   }

   private Single<ResultMessage> executeWithoutCondition(QueryState queryState, QueryOptions options, long queryStartNanoTime) {
      ConsistencyLevel cl = options.getConsistency();

      try {
         if(this.isCounter()) {
            cl.validateCounterForWrite(this.metadata());
         } else {
            cl.validateForWrite(this.metadata.keyspace);
         }
      } catch (InvalidRequestException var7) {
         return Single.error(var7);
      }

      return this.getMutations(options, false, options.getTimestamp(queryState), queryStartNanoTime).flatMap((mutations) -> {
         return !mutations.isEmpty()?StorageProxy.mutateWithTriggers(mutations, cl, false, queryStartNanoTime):Single.just(new ResultMessage.Void());
      });
   }

   public Single<ResultMessage> executeWithCondition(QueryState queryState, QueryOptions options, long queryStartNanoTime) throws RequestExecutionException, RequestValidationException {
      CQL3CasRequest request = this.makeCasRequest(queryState, options);
      Single<ResultMessage> result = Single.defer(() -> {
         return this.buildCasResultSet(StorageProxy.cas(this.keyspace(), this.columnFamily(), request.key, request, options.getSerialConsistency(), options.getConsistency(), queryState.getClientState(), queryStartNanoTime), options);
      }).map(ResultMessage.Rows::new);
      return RxThreads.subscribeOnIo(result, TPCTaskType.CAS);
   }

   private CQL3CasRequest makeCasRequest(QueryState queryState, QueryOptions options) {
      List<ByteBuffer> keys = this.buildPartitionKeyNames(options);
      RequestValidations.checkFalse(this.restrictions.keyIsInRelation(), "IN on the partition key is not supported with conditional %s", this.type.isUpdate()?"updates":"deletions");
      DecoratedKey key = this.metadata().partitioner.decorateKey((ByteBuffer)keys.get(0));
      long now = options.getTimestamp(queryState);
      RequestValidations.checkFalse(this.restrictions.clusteringKeyRestrictionsHasIN(), "IN on the clustering key columns is not supported with conditional %s", this.type.isUpdate()?"updates":"deletions");
      Clustering clustering = (Clustering)Iterables.getOnlyElement(this.createClustering(options));
      CQL3CasRequest request = new CQL3CasRequest(this.metadata(), key, false, this.conditionColumns(), this.updatesRegularRows(), this.updatesStaticRow());
      this.addConditions(clustering, request, options);
      request.addRowUpdate(clustering, this, options, now);
      return request;
   }

   public void addConditions(Clustering clustering, CQL3CasRequest request, QueryOptions options) throws InvalidRequestException {
      this.conditions.addConditionsTo(request, clustering, options);
   }

   private static ResultSet.ResultMetadata buildCASSuccessMetadata(String ksName, String cfName) {
      List<ColumnSpecification> specs = new ArrayList();
      specs.add(casResultColumnSpecification(ksName, cfName));
      return new ResultSet.ResultMetadata(EMPTY_HASH, specs);
   }

   private static ColumnSpecification casResultColumnSpecification(String ksName, String cfName) {
      return new ColumnSpecification(ksName, cfName, CAS_RESULT_COLUMN, BooleanType.instance);
   }

   private Single<ResultSet> buildCasResultSet(Optional<RowIterator> partition, QueryOptions options) throws InvalidRequestException {
      return buildCasResultSet(this.keyspace(), this.columnFamily(), partition, this.getColumnsWithConditions(), false, options);
   }

   public static Single<ResultSet> buildCasResultSet(String ksName, String tableName, Optional<RowIterator> partition, Iterable<ColumnMetadata> columnsWithConditions, boolean isBatch, QueryOptions options) throws InvalidRequestException {
      boolean success = !partition.isPresent();
      ResultSet.ResultMetadata metadata = buildCASSuccessMetadata(ksName, tableName);
      List<List<ByteBuffer>> rows = Collections.singletonList(Collections.singletonList(BooleanType.instance.decompose(Boolean.valueOf(success))));
      ResultSet rs = new ResultSet(metadata, rows);
      return success?Single.just(rs):buildCasFailureResultSet((RowIterator)partition.get(), columnsWithConditions, isBatch, options).map((failure) -> {
         return merge(rs, failure);
      });
   }

   private static ResultSet merge(ResultSet left, ResultSet right) {
      if(left.size() == 0) {
         return right;
      } else if(right.size() == 0) {
         return left;
      } else {
         assert left.size() == 1;

         int size = left.metadata.names.size() + right.metadata.names.size();
         List<ColumnSpecification> specs = new ArrayList(size);
         specs.addAll(left.metadata.names);
         specs.addAll(right.metadata.names);
         List<List<ByteBuffer>> rows = new ArrayList(right.size());

         for(int i = 0; i < right.size(); ++i) {
            List<ByteBuffer> row = new ArrayList(size);
            row.addAll((Collection)left.rows.get(0));
            row.addAll((Collection)right.rows.get(i));
            rows.add(row);
         }

         return new ResultSet(new ResultSet.ResultMetadata(EMPTY_HASH, specs), rows);
      }
   }

   private static Single<ResultSet> buildCasFailureResultSet(RowIterator partition, Iterable<ColumnMetadata> columnsWithConditions, boolean isBatch, QueryOptions options) throws InvalidRequestException {
      TableMetadata metadata = partition.metadata();
      Selection selection;
      if(columnsWithConditions == null) {
         selection = Selection.wildcard(metadata, false);
      } else {
         Set<ColumnMetadata> defs = new LinkedHashSet();
         if(isBatch) {
            Iterables.addAll(defs, metadata.primaryKeyColumns());
         }

         Iterables.addAll(defs, columnsWithConditions);
         selection = Selection.forColumns(metadata, new ArrayList(defs));
      }

      ResultSet.Builder builder = ResultSet.makeBuilder(selection.getResultMetadata(), selection.newSelectors(options));
      return SelectStatement.forSelection(metadata, selection).processPartition(FlowablePartitions.fromIterator(partition, (StagedScheduler)null), options, builder, FBUtilities.nowInSeconds()).mapToRxSingle(ResultSet.Builder::build);
   }

   public Single<ResultMessage> executeInternal(QueryState queryState, QueryOptions options) throws RequestValidationException, RequestExecutionException {
      return this.hasConditions()?this.executeInternalWithCondition(queryState, options):this.executeInternalWithoutCondition(queryState, options, System.nanoTime());
   }

   private Single<ResultMessage> executeInternalWithoutCondition(QueryState queryState, QueryOptions options, long queryStartNanoTime) throws RequestValidationException, RequestExecutionException {
      return this.getMutations(options, true, queryState.getTimestamp(), queryStartNanoTime).flatMapCompletable((mutations) -> {
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

   private Single<ResultMessage> executeInternalWithCondition(QueryState state, QueryOptions options) throws RequestValidationException, RequestExecutionException {
      CQL3CasRequest request = this.makeCasRequest(state, options);
      return casInternal(request, state).flatMap((result) -> {
         return this.buildCasResultSet(result, options);
      }).map(ResultMessage.Rows::new);
   }

   static Single<Optional<RowIterator>> casInternal(CQL3CasRequest request, QueryState state) {
      UUID ballot = UUIDGen.getTimeUUIDFromMicros(state.getTimestamp());
      SinglePartitionReadCommand readCommand = request.readCommand(FBUtilities.nowInSeconds());
      return readCommand.executeInternal().flatMap((partition) -> {
         return FilteredPartition.create(partition);
      }).mapToRxSingle().flatMap((current) -> {
         if(current == null) {
            current = FilteredPartition.empty(readCommand);
         }

         if(!request.appliesTo(current)) {
            return Single.just(Optional.of(current.rowIterator()));
         } else {
            PartitionUpdate updates = request.makeUpdates(current);
            updates = TriggerExecutor.instance.execute(updates);
            Commit proposal = Commit.newProposal(ballot, updates);
            return proposal.makeMutation().applyAsync().toSingleDefault(Optional.empty());
         }
      });
   }

   private Single<Collection<? extends IMutation>> getMutations(QueryOptions options, boolean local, long now, long queryStartNanoTime) {
      UpdatesCollector collector = new UpdatesCollector(Collections.singletonMap(this.metadata.id, this.updatedColumns), 1);
      return this.addUpdates(collector, options, local, now, queryStartNanoTime).andThen(Single.fromCallable(() -> {
         collector.validateIndexedColumns();
         return collector.toMutations();
      }));
   }

   final Completable addUpdates(UpdatesCollector collector, QueryOptions options, boolean local, long now, long queryStartNanoTime) {
      List<ByteBuffer> keys = this.buildPartitionKeyNames(options);
      if (this.hasSlices()) {
         Slices slices = this.createSlices(options);
         if (slices.isEmpty()) {
            return Completable.complete();
         }
         Single<UpdateParameters> paramsSingle = this.makeUpdateParameters(keys, new ClusteringIndexSliceFilter(slices, false), options, DataLimits.NONE, local, now, queryStartNanoTime);
         return paramsSingle.flatMapCompletable(params -> {
            for (ByteBuffer key : keys) {
               Validation.validateKey(this.metadata(), key);
               DecoratedKey dk = this.metadata().partitioner.decorateKey(key);
               PartitionUpdate upd = collector.getPartitionUpdate(this.metadata(), dk, options.getConsistency());
               for (Slice slice : slices) {
                  this.addUpdateForKey(upd, slice, params);
               }
            }
            return Completable.complete();
         });
      }
      NavigableSet<Clustering> clusterings = this.createClustering(options);
      if (this.restrictions.hasClusteringColumnsRestrictions() && clusterings.isEmpty()) {
         return Completable.complete();
      }
      Single<UpdateParameters> paramsSingle = this.makeUpdateParameters(keys, clusterings, options, local, now, queryStartNanoTime);
      return paramsSingle.flatMapCompletable(params -> {
         for (ByteBuffer key : keys) {
            Validation.validateKey(this.metadata(), key);
            DecoratedKey dk = this.metadata().partitioner.decorateKey(key);
            PartitionUpdate upd = collector.getPartitionUpdate(this.metadata, dk, options.getConsistency());
            if (!this.restrictions.hasClusteringColumnsRestrictions()) {
               this.addUpdateForKey(upd, Clustering.EMPTY, params);
               continue;
            }
            for (Clustering clustering : clusterings) {
               for (ByteBuffer c : clustering.getRawValues()) {
                  if (c == null || c.remaining() <= 65535) continue;
                  throw new InvalidRequestException(String.format("Key length of %d is longer than maximum of %d", clustering.dataSize(), 65535));
               }
               this.addUpdateForKey(upd, clustering, params);
            }
         }
         return Completable.complete();
      });
   }

   Slices createSlices(QueryOptions options) {
      SortedSet<ClusteringBound> startBounds = this.restrictions.getClusteringColumnsBounds(Bound.START, options);
      SortedSet<ClusteringBound> endBounds = this.restrictions.getClusteringColumnsBounds(Bound.END, options);
      return this.toSlices(startBounds, endBounds);
   }

   private Single<UpdateParameters> makeUpdateParameters(Collection<ByteBuffer> keys, NavigableSet<Clustering> clusterings, QueryOptions options, boolean local, long now, long queryStartNanoTime) {
      return clusterings.contains(Clustering.STATIC_CLUSTERING)?this.makeUpdateParameters(keys, new ClusteringIndexSliceFilter(Slices.ALL, false), options, DataLimits.cqlLimits(1), local, now, queryStartNanoTime):this.makeUpdateParameters(keys, new ClusteringIndexNamesFilter(clusterings, false), options, DataLimits.NONE, local, now, queryStartNanoTime);
   }

   private Single<UpdateParameters> makeUpdateParameters(Collection<ByteBuffer> keys, ClusteringIndexFilter filter, QueryOptions options, DataLimits limits, boolean local, long now, long queryStartNanoTime) {
      return this.readRequiredLists(keys, filter, limits, local, options.getConsistency(), queryStartNanoTime).map((lists) -> {
         return new UpdateParameters(this.metadata(), this.updatedColumns(), options, this.getTimestamp(now, options), this.getTimeToLive(options), lists);
      });
   }

   private Slices toSlices(SortedSet<ClusteringBound> startBounds, SortedSet<ClusteringBound> endBounds) {
      assert startBounds.size() == endBounds.size();

      Slices.Builder builder = new Slices.Builder(this.metadata().comparator);
      Iterator<ClusteringBound> starts = startBounds.iterator();
      Iterator ends = endBounds.iterator();

      while(starts.hasNext()) {
         Slice slice = Slice.make((ClusteringBound)starts.next(), (ClusteringBound)ends.next());
         if(!slice.isEmpty(this.metadata().comparator)) {
            builder.add(slice);
         }
      }

      return builder.build();
   }

   public abstract static class Parsed extends CFStatement {
      protected final StatementType type;
      private final Attributes.Raw attrs;
      private final List<Pair<ColumnMetadata.Raw, ColumnCondition.Raw>> conditions;
      private final boolean ifNotExists;
      private final boolean ifExists;

      protected Parsed(CFName name, StatementType type, Attributes.Raw attrs, List<Pair<ColumnMetadata.Raw, ColumnCondition.Raw>> conditions, boolean ifNotExists, boolean ifExists) {
         super(name);
         this.type = type;
         this.attrs = attrs;
         this.conditions = conditions == null?Collections.emptyList():conditions;
         this.ifNotExists = ifNotExists;
         this.ifExists = ifExists;
      }

      public void prepareKeyspace(ClientState state) throws InvalidRequestException {
         super.prepareKeyspace(state);
      }

      public ParsedStatement.Prepared prepare() {
         VariableSpecifications boundNames = this.getBoundVariables();
         ModificationStatement statement = this.prepare(boundNames);
         TableMetadata metadata = Schema.instance.validateTable(this.keyspace(), this.columnFamily());
         return new ParsedStatement.Prepared(statement, boundNames, boundNames.getPartitionKeyBindIndexes(metadata));
      }

      public ModificationStatement prepare(VariableSpecifications boundNames) {
         TableMetadata metadata = Schema.instance.validateTable(this.keyspace(), this.columnFamily());
         Attributes preparedAttributes = this.attrs.prepare(this.keyspace(), this.columnFamily());
         preparedAttributes.collectMarkerSpecification(boundNames);
         Conditions preparedConditions = this.prepareConditions(metadata, boundNames);
         return this.prepareInternal(metadata, boundNames, preparedConditions, preparedAttributes);
      }

      private Conditions prepareConditions(TableMetadata metadata, VariableSpecifications boundNames) {
         if(this.ifExists) {
            assert this.conditions.isEmpty();

            assert !this.ifNotExists;

            return Conditions.IF_EXISTS_CONDITION;
         } else if(this.ifNotExists) {
            assert this.conditions.isEmpty();

            assert !this.ifExists;

            return Conditions.IF_NOT_EXISTS_CONDITION;
         } else {
            return (Conditions)(this.conditions.isEmpty()?Conditions.EMPTY_CONDITION:this.prepareColumnConditions(metadata, boundNames));
         }
      }

      private ColumnConditions prepareColumnConditions(TableMetadata metadata, VariableSpecifications boundNames) {
         RequestValidations.checkNull(this.attrs.timestamp, "Cannot provide custom timestamp for conditional updates");
         ColumnConditions.Builder builder = ColumnConditions.newBuilder();
         Iterator var4 = this.conditions.iterator();

         while(var4.hasNext()) {
            Pair<ColumnMetadata.Raw, ColumnCondition.Raw> entry = (Pair)var4.next();
            ColumnMetadata def = ((ColumnMetadata.Raw)entry.left).prepare(metadata);
            ColumnCondition condition = ((ColumnCondition.Raw)entry.right).prepare(this.keyspace(), def, metadata);
            condition.collectMarkerSpecification(boundNames);
            RequestValidations.checkFalse(def.isPrimaryKeyColumn(), "PRIMARY KEY column '%s' cannot have IF conditions", def.name);
            builder.add(condition);
         }

         return builder.build();
      }

      protected abstract ModificationStatement prepareInternal(TableMetadata var1, VariableSpecifications var2, Conditions var3, Attributes var4);

      protected StatementRestrictions newRestrictions(TableMetadata metadata, VariableSpecifications boundNames, Operations operations, WhereClause where, Conditions conditions) {
         if(where.containsCustomExpressions()) {
            throw new InvalidRequestException("Custom index expressions cannot be used in WHERE clauses for UPDATE or DELETE statements");
         } else {
            boolean applyOnlyToStaticColumns = ModificationStatement.appliesOnlyToStaticColumns(operations, conditions);
            return new StatementRestrictions(this.type, metadata, where, boundNames, applyOnlyToStaticColumns, false, false);
         }
      }

      protected static ColumnMetadata getColumnDefinition(TableMetadata metadata, ColumnMetadata.Raw rawId) {
         return rawId.prepare(metadata);
      }
   }
}
