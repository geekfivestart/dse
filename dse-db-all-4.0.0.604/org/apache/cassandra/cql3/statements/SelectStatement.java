package org.apache.cassandra.cql3.statements;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;
import com.google.common.base.MoreObjects;
import com.google.common.collect.UnmodifiableIterator;
import io.reactivex.Single;
import io.reactivex.functions.BooleanSupplier;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCRunnable;
import org.apache.cassandra.concurrent.TPCScheduler;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.cql3.PagingResult;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.WhereClause;
import org.apache.cassandra.cql3.continuous.paging.ContinuousBackPressureException;
import org.apache.cassandra.cql3.continuous.paging.ContinuousPagingService;
import org.apache.cassandra.cql3.continuous.paging.ContinuousPagingState;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.restrictions.ExternalRestriction;
import org.apache.cassandra.cql3.restrictions.Restrictions;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.selection.RawSelector;
import org.apache.cassandra.cql3.selection.ResultBuilder;
import org.apache.cassandra.cql3.selection.Selectable;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.cql3.selection.Selector;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadContext;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.aggregation.AggregationSpecification;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.MultiCellType;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.db.rows.FlowablePartitions;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.exceptions.CassandraException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.sasi.SASIIndex;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.pager.AggregationQueryPager;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.flow.Flow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SelectStatement implements CQLStatement, TableStatement {
   private static final Logger logger = LoggerFactory.getLogger(SelectStatement.class);
   private static final PageSize DEFAULT_PAGE_SIZE = PageSize.rowsSize(10000);
   private final int boundTerms;
   public final TableMetadata table;
   public final SelectStatement.Parameters parameters;
   private final Selection selection;
   private final Term limit;
   private final Term perPartitionLimit;
   private final StatementRestrictions restrictions;
   private final boolean isReversed;
   private final AggregationSpecification.Factory aggregationSpecFactory;
   private final Comparator<List<ByteBuffer>> orderingComparator;
   private static final SelectStatement.Parameters defaultParameters = new SelectStatement.Parameters(Collections.emptyMap(), Collections.emptyList(), false, false, false);

   public SelectStatement(TableMetadata table, int boundTerms, SelectStatement.Parameters parameters, Selection selection, StatementRestrictions restrictions, boolean isReversed, AggregationSpecification.Factory aggregationSpecFactory, Comparator<List<ByteBuffer>> orderingComparator, Term limit, Term perPartitionLimit) {
      this.table = table;
      this.boundTerms = boundTerms;
      this.selection = selection;
      this.restrictions = restrictions;
      this.isReversed = isReversed;
      this.aggregationSpecFactory = aggregationSpecFactory;
      this.orderingComparator = orderingComparator;
      this.parameters = parameters;
      this.limit = limit;
      this.perPartitionLimit = perPartitionLimit;
   }

   public AuditableEventType getAuditEventType() {
      return CoreAuditableEventType.CQL_SELECT;
   }

   public SelectStatement addIndexRestrictions(Restrictions indexRestrictions) {
      return new SelectStatement(this.table, this.boundTerms, this.parameters, this.selection, this.restrictions.addIndexRestrictions(indexRestrictions), this.isReversed, this.aggregationSpecFactory, this.orderingComparator, this.limit, this.perPartitionLimit);
   }

   public SelectStatement addIndexRestrictions(Iterable<ExternalRestriction> indexRestrictions) {
      return new SelectStatement(this.table, this.boundTerms, this.parameters, this.selection, this.restrictions.addExternalRestrictions(indexRestrictions), this.isReversed, this.aggregationSpecFactory, this.orderingComparator, this.limit, this.perPartitionLimit);
   }

   public final List<ColumnSpecification> getSelectedColumns() {
      return this.selection.getSelectedColumns();
   }

   public Iterable<Function> getFunctions() {
      List<Function> functions = new ArrayList();
      this.addFunctionsTo(functions);
      return functions;
   }

   private void addFunctionsTo(List<Function> functions) {
      this.selection.addFunctionsTo(functions);
      this.restrictions.addFunctionsTo(functions);
      if(this.aggregationSpecFactory != null) {
         this.aggregationSpecFactory.addFunctionsTo(functions);
      }

      if(this.limit != null) {
         this.limit.addFunctionsTo(functions);
      }

      if(this.perPartitionLimit != null) {
         this.perPartitionLimit.addFunctionsTo(functions);
      }

   }

   public ColumnFilter queriedColumns() {
      return this.selection.newSelectors(QueryOptions.DEFAULT).getColumnFilter();
   }

   static SelectStatement forSelection(TableMetadata table, Selection selection) {
      return new SelectStatement(table, 0, defaultParameters, selection, StatementRestrictions.empty(StatementType.SELECT, table), false, (AggregationSpecification.Factory)null, (Comparator)null, (Term)null, (Term)null);
   }

   public ResultSet.ResultMetadata getResultMetadata() {
      return this.selection.getResultMetadata();
   }

   public int getBoundTerms() {
      return this.boundTerms;
   }

   public void checkAccess(QueryState state) {
      if(this.table.isView()) {
         TableMetadataRef baseTable = View.findBaseTable(this.keyspace(), this.columnFamily());
         if(baseTable != null) {
            state.checkTablePermission((TableMetadataRef)baseTable, CorePermission.SELECT);
         }
      } else {
         state.checkTablePermission((TableMetadata)this.table, CorePermission.SELECT);
      }

      Iterator var4 = this.getFunctions().iterator();

      while(var4.hasNext()) {
         Function function = (Function)var4.next();
         state.checkFunctionPermission((Function)function, CorePermission.EXECUTE);
      }

   }

   public void validate(QueryState state) throws InvalidRequestException {
   }

   public Single<ResultMessage.Rows> execute(QueryState state, QueryOptions options, long queryStartNanoTime) {
      ConsistencyLevel cl = options.getConsistency();
      RequestValidations.checkNotNull(cl, "Invalid empty consistency level");
      cl.validateForRead(this.keyspace());
      if(options.continuousPagesRequested()) {
         RequestValidations.checkNotNull(state.getConnection(), "Continuous paging should only be used for external queries");
         RequestValidations.checkFalse(cl.isSerialConsistency(), "Continuous paging does not support serial reads");
         return this.executeContinuous(state, options, FBUtilities.nowInSeconds(), queryStartNanoTime);
      } else {
         return this.execute(state, options, FBUtilities.nowInSeconds(), queryStartNanoTime);
      }
   }

   public StagedScheduler getScheduler() {
      return null;
   }

   private PageSize getPageSize(QueryOptions options) {
      QueryOptions.PagingOptions pagingOptions = options.getPagingOptions();
      return pagingOptions == null?PageSize.NULL:pagingOptions.pageSize();
   }

   public AggregationSpecification getAggregationSpec(QueryOptions options) {
      return this.aggregationSpecFactory == null?null:this.aggregationSpecFactory.newInstance(options);
   }

   public ReadQuery getQuery(QueryState queryState, QueryOptions options, int nowInSec) throws RequestValidationException {
      Selection.Selectors selectors = this.selection.newSelectors(options);
      DataLimits limit = this.getDataLimits(this.getLimit(options), this.getPerPartitionLimit(options), this.getPageSize(options), this.getAggregationSpec(options));
      return this.getQuery(queryState, options, selectors.getColumnFilter(), nowInSec, limit);
   }

   public ReadQuery getQuery(QueryState queryState, QueryOptions options, ColumnFilter columnFilter, int nowInSec, DataLimits limit) {
      return this.isPartitionRangeQuery()?this.getRangeCommand(queryState, options, columnFilter, limit, nowInSec):this.getSliceCommands(queryState, options, columnFilter, limit, nowInSec);
   }

   private Single<ResultMessage.Rows> execute(ReadQuery query, QueryOptions options, ReadContext ctx, Selection.Selectors selectors, int nowInSec, int userLimit) throws RequestValidationException, RequestExecutionException {
      Flow<FlowablePartition> data = query.execute(ctx);
      return this.processResults(data, options, selectors, nowInSec, userLimit, (AggregationSpecification)null);
   }

   private boolean isPartitionRangeQuery() {
      return this.restrictions.isKeyRange() || this.restrictions.usesSecondaryIndexing();
   }

   private Single<ResultMessage.Rows> execute(SelectStatement.Pager pager, QueryOptions options, Selection.Selectors selectors, PageSize pageSize, int nowInSec, int userLimit, AggregationSpecification aggregationSpec, long queryStartNanoTime) throws RequestValidationException, RequestExecutionException {
      if(this.aggregationSpecFactory != null) {
         if(!this.restrictions.hasPartitionKeyRestrictions()) {
            this.warn(String.format("Aggregation query used without partition key (ks: %s, tbl: %s)", new Object[]{this.table.keyspace, this.table.name}), "Aggregation query used without partition key");
         } else if(this.restrictions.keyIsInRelation()) {
            this.warn(String.format("Aggregation query used on multiple partition keys (IN restriction) (ks: %s, tbl: %s)", new Object[]{this.table.keyspace, this.table.name}), "Aggregation query used on multiple partition keys");
         }
      }

      RequestValidations.checkFalse(pageSize != PageSize.NULL && this.needsPostQueryOrdering(), "Cannot page queries with both ORDER BY and a IN restriction on the partition key; you must either remove the ORDER BY or the IN and sort client side, or disable paging for this query");
      Flow<FlowablePartition> page = pager.fetchPage(pageSize, queryStartNanoTime);
      Single<ResultMessage.Rows> msg = this.processResults(page, options, selectors, nowInSec, userLimit, aggregationSpec).map((r) -> {
         if(!pager.isExhausted()) {
            r.result.metadata.setPagingResult(new PagingResult(pager.state(false)));
         }

         return r;
      });
      return msg;
   }

   private void warn(String msg) {
      this.warn(msg, msg);
   }

   private void warn(String serverMessage, String clientMessage) {
      logger.warn("{}", serverMessage);
      ClientWarn.instance.warn(clientMessage);
   }

   private Single<ResultMessage.Rows> processResults(Flow<FlowablePartition> partitions, QueryOptions options, Selection.Selectors selectors, int nowInSec, int userLimit, AggregationSpecification aggregationSpec) throws RequestValidationException {
      return this.process(partitions, options, selectors, nowInSec, userLimit, aggregationSpec).map(ResultMessage.Rows::<init>);
   }

   public Single<ResultMessage.Rows> executeInternal(QueryState state, QueryOptions options) throws RequestExecutionException, RequestValidationException {
      return this.executeInternal(state, options, FBUtilities.nowInSeconds(), System.nanoTime());
   }

   public Single<ResultMessage.Rows> executeInternal(QueryState state, QueryOptions options, int nowInSec, long queryStartNanoTime) throws RequestExecutionException, RequestValidationException {
      Selection.Selectors selectors = this.selection.newSelectors(options);
      AggregationSpecification aggregationSpec = this.getAggregationSpec(options);
      int userLimit = this.getLimit(options);
      int userPerPartitionLimit = this.getPerPartitionLimit(options);
      PageSize pageSize = this.getPageSize(options);
      DataLimits limit = this.getDataLimits(userLimit, userPerPartitionLimit, pageSize, aggregationSpec);
      ReadQuery query = this.getQuery(state, options, selectors.getColumnFilter(), nowInSec, limit);
      if(aggregationSpec == null && pageSize.isLarger(query.limits().count())) {
         return this.processResults(query.executeInternal(), options, selectors, nowInSec, userLimit, (AggregationSpecification)null);
      } else {
         QueryPager pager = this.getPager(query, options);
         return this.execute(SelectStatement.Pager.forInternalQuery(pager), options, selectors, pageSize, nowInSec, userLimit, aggregationSpec, queryStartNanoTime);
      }
   }

   private Single<ResultMessage.Rows> execute(QueryState state, QueryOptions options, int nowInSec, long queryStartNanoTime) throws RequestExecutionException, RequestValidationException {
      Selection.Selectors selectors = this.selection.newSelectors(options);
      AggregationSpecification aggregationSpec = this.getAggregationSpec(options);
      int userLimit = this.getLimit(options);
      int userPerPartitionLimit = this.getPerPartitionLimit(options);
      PageSize pageSize = this.getPageSize(options);
      DataLimits limit = this.getDataLimits(userLimit, userPerPartitionLimit, pageSize, aggregationSpec);
      ReadQuery query = this.getQuery(state, options, selectors.getColumnFilter(), nowInSec, limit);
      ReadContext.Builder builder = ReadContext.builder(query, options.getConsistency()).state(state.getClientState());
      if(aggregationSpec == null && pageSize.isLarger(query.limits().count())) {
         return this.execute(query, options, builder.build(queryStartNanoTime), selectors, nowInSec, userLimit);
      } else {
         QueryPager pager = this.getPager(query, options);
         return this.execute(SelectStatement.Pager.forNormalQuery(pager, builder), options, selectors, pageSize, nowInSec, userLimit, aggregationSpec, queryStartNanoTime);
      }
   }

   private Single<ResultMessage.Rows> executeContinuous(QueryState queryState, QueryOptions queryOptions, int nowInSec, long queryStartNanoTime) throws RequestValidationException, RequestExecutionException {
      ContinuousPagingService.metrics.requests.mark();
      RequestValidations.checkFalse(this.needsPostQueryOrdering(), "Cannot page queries with both ORDER BY and a IN restriction on the partition key; you must either remove the ORDER BY or the IN and sort client side, or avoid async paging for this query");
      Selection.Selectors selectors = this.selection.newSelectors(queryOptions);
      AggregationSpecification aggregationSpec = this.getAggregationSpec(queryOptions);
      int userLimit = this.getLimit(queryOptions);
      int userPerPartitionLimit = this.getPerPartitionLimit(queryOptions);
      PageSize pageSize = this.getPageSize(queryOptions);
      DataLimits limit = this.getDataLimits(userLimit, userPerPartitionLimit, pageSize, aggregationSpec);
      ReadQuery query = this.getQuery(queryState, queryOptions, selectors.getColumnFilter(), nowInSec, limit);
      ContinuousPagingState continuousPagingState = new ContinuousPagingState(DatabaseDescriptor.getContinuousPaging(), new SelectStatement.ContinuousPagingExecutor(this, queryOptions, queryState, query, queryStartNanoTime, pageSize, null), () -> {
         return queryState.getConnection().channel();
      }, ResultSet.estimatedRowSizeForColumns(this.table, this.getSelection().getColumnMapping()));
      ResultBuilder builder = ContinuousPagingService.createSession(this.getSelection().newSelectors(queryOptions), aggregationSpec == null?null:aggregationSpec.newGroupMaker(), this.getResultMetadata(), continuousPagingState, queryState, queryOptions);
      continuousPagingState.executor.schedule(queryOptions.getPagingOptions().state(), builder);
      return Single.just(new ResultMessage.Rows(new ResultSet(this.getResultMetadata(), Collections.emptyList()), false));
   }

   private QueryPager getPager(ReadQuery query, QueryOptions options) {
      PagingState pagingState = options.getPagingOptions() == null?null:options.getPagingOptions().state();
      return this.getPager(query, pagingState, options.getProtocolVersion());
   }

   private QueryPager getPager(ReadQuery query, PagingState pagingState, ProtocolVersion protocolVersion) {
      QueryPager pager = query.getPager(pagingState, protocolVersion);
      return (QueryPager)(this.aggregationSpecFactory != null && !query.isEmpty()?new AggregationQueryPager(pager, query.limits(), DatabaseDescriptor.getAggregatedQueryTimeout(), ResultSet.estimatedRowSizeForAllColumns(this.table)):pager);
   }

   public ResultSet process(PartitionIterator partitions, int nowInSec) {
      return (ResultSet)this.process(FlowablePartitions.fromPartitions(partitions, TPC.ioScheduler()), nowInSec).blockingGet();
   }

   public Single<ResultSet> process(Flow<FlowablePartition> partitions, int nowInSec) {
      return this.process(partitions, QueryOptions.DEFAULT, this.selection.newSelectors(QueryOptions.DEFAULT), nowInSec, this.getLimit(QueryOptions.DEFAULT), this.getAggregationSpec(QueryOptions.DEFAULT));
   }

   public String keyspace() {
      return this.table.keyspace;
   }

   public String columnFamily() {
      return this.table.name;
   }

   public Selection getSelection() {
      return this.selection;
   }

   public StatementRestrictions getRestrictions() {
      return this.restrictions;
   }

   private ReadQuery getSliceCommands(QueryState queryState, QueryOptions options, ColumnFilter columnFilter, DataLimits limit, int nowInSec) {
      Collection<ByteBuffer> keys = this.restrictions.getPartitionKeys(options);
      if(keys.isEmpty()) {
         return ReadQuery.empty(this.table);
      } else {
         ClusteringIndexFilter filter = this.makeClusteringIndexFilter(options, columnFilter);
         if(filter == null) {
            return ReadQuery.empty(this.table);
         } else {
            RowFilter rowFilter = this.getRowFilter(queryState, options);
            List<SinglePartitionReadCommand> commands = new ArrayList(keys.size());
            Iterator var10 = keys.iterator();

            while(var10.hasNext()) {
               ByteBuffer key = (ByteBuffer)var10.next();
               QueryProcessor.validateKey(key);
               DecoratedKey dk = this.table.partitioner.decorateKey(ByteBufferUtil.clone(key));
               commands.add(SinglePartitionReadCommand.create(this.table, nowInSec, columnFilter, rowFilter, limit, dk, filter));
            }

            return new SinglePartitionReadCommand.Group(commands, limit);
         }
      }
   }

   public Slices clusteringIndexFilterAsSlices() {
      QueryOptions options = QueryOptions.forInternalCalls(Collections.emptyList());
      ColumnFilter columnFilter = this.selection.newSelectors(options).getColumnFilter();
      ClusteringIndexFilter filter = this.makeClusteringIndexFilter(options, columnFilter);
      if(filter instanceof ClusteringIndexSliceFilter) {
         return ((ClusteringIndexSliceFilter)filter).requestedSlices();
      } else {
         Slices.Builder builder = new Slices.Builder(this.table.comparator);
         Iterator var5 = ((ClusteringIndexNamesFilter)filter).requestedRows().iterator();

         while(var5.hasNext()) {
            Clustering clustering = (Clustering)var5.next();
            builder.add(Slice.make(clustering));
         }

         return builder.build();
      }
   }

   public SinglePartitionReadCommand internalReadForView(DecoratedKey key, int nowInSec) {
      QueryOptions options = QueryOptions.forInternalCalls(Collections.emptyList());
      ColumnFilter columnFilter = this.selection.newSelectors(options).getColumnFilter();
      ClusteringIndexFilter filter = this.makeClusteringIndexFilter(options, columnFilter);
      RowFilter rowFilter = this.getRowFilter(QueryState.forInternalCalls(), options);
      return SinglePartitionReadCommand.create(this.table, nowInSec, columnFilter, rowFilter, DataLimits.NONE, key, filter);
   }

   public RowFilter rowFilterForInternalCalls() {
      return this.getRowFilter(QueryState.forInternalCalls(), QueryOptions.forInternalCalls(Collections.emptyList()));
   }

   private ReadQuery getRangeCommand(QueryState queryState, QueryOptions options, ColumnFilter columnFilter, DataLimits limit, int nowInSec) {
      ClusteringIndexFilter clusteringIndexFilter = this.makeClusteringIndexFilter(options, columnFilter);
      if(clusteringIndexFilter == null) {
         return ReadQuery.empty(this.table);
      } else {
         RowFilter rowFilter = this.getRowFilter(queryState, options);
         AbstractBounds<PartitionPosition> keyBounds = this.restrictions.getPartitionKeyBounds(options);
         if(keyBounds == null) {
            return ReadQuery.empty(this.table);
         } else {
            PartitionRangeReadCommand command = PartitionRangeReadCommand.create(this.table, nowInSec, columnFilter, rowFilter, limit, new DataRange(keyBounds, clusteringIndexFilter));
            command.maybeValidateIndex();
            ClientState clientState = queryState.getClientState();
            if(!clientState.isInternal && !clientState.isSASIWarningIssued() && command.getIndex(Keyspace.open(this.table.keyspace).getColumnFamilyStore(this.table.name)) instanceof SASIIndex) {
               this.warn(String.format("SASI index was enabled for '%s.%s'. SASI is still in beta, take extra caution when using it in production.", new Object[]{this.table.keyspace, this.table.name}));
               clientState.setSASIWarningIssued();
            }

            return command;
         }
      }
   }

   private ClusteringIndexFilter makeClusteringIndexFilter(QueryOptions options, ColumnFilter columnFilter) {
      if(this.parameters.isDistinct) {
         return new ClusteringIndexSliceFilter(Slices.ALL, false);
      } else if(this.restrictions.isColumnRange()) {
         Slices slices = this.makeSlices(options);
         return slices == Slices.NONE && !this.selection.containsStaticColumns()?null:new ClusteringIndexSliceFilter(slices, this.isReversed);
      } else {
         NavigableSet<Clustering> clusterings = this.getRequestedRows(options);
         return clusterings.isEmpty() && columnFilter.fetchedColumns().statics.isEmpty()?null:new ClusteringIndexNamesFilter(clusterings, this.isReversed);
      }
   }

   private Slices makeSlices(QueryOptions options) throws InvalidRequestException {
      SortedSet<ClusteringBound> startBounds = this.restrictions.getClusteringColumnsBounds(Bound.START, options);
      SortedSet<ClusteringBound> endBounds = this.restrictions.getClusteringColumnsBounds(Bound.END, options);

      assert startBounds.size() == endBounds.size();

      if(startBounds.size() == 1) {
         ClusteringBound start = (ClusteringBound)startBounds.first();
         ClusteringBound end = (ClusteringBound)endBounds.first();
         return this.table.comparator.compare((ClusteringPrefix)start, (ClusteringPrefix)end) > 0?Slices.NONE:Slices.with(this.table.comparator, Slice.make(start, end));
      } else {
         Slices.Builder builder = new Slices.Builder(this.table.comparator, startBounds.size());
         Iterator<ClusteringBound> startIter = startBounds.iterator();
         Iterator endIter = endBounds.iterator();

         while(startIter.hasNext() && endIter.hasNext()) {
            ClusteringBound start = (ClusteringBound)startIter.next();
            ClusteringBound end = (ClusteringBound)endIter.next();
            if(this.table.comparator.compare((ClusteringPrefix)start, (ClusteringPrefix)end) <= 0) {
               builder.add(start, end);
            }
         }

         return builder.build();
      }
   }

   private DataLimits getDataLimits(int userLimit, int perPartitionLimit, PageSize pageSize, AggregationSpecification aggregationSpec) {
      int cqlRowLimit = 2147483647;
      int cqlPerPartitionLimit = 2147483647;
      if(aggregationSpec != AggregationSpecification.AGGREGATE_EVERYTHING) {
         if(!this.needsPostQueryOrdering()) {
            cqlRowLimit = userLimit;
         }

         cqlPerPartitionLimit = perPartitionLimit;
      }

      if(pageSize == PageSize.NULL) {
         pageSize = DEFAULT_PAGE_SIZE;
      }

      return aggregationSpec != null && aggregationSpec != AggregationSpecification.AGGREGATE_EVERYTHING?(this.parameters.isDistinct?DataLimits.distinctLimits(cqlRowLimit):DataLimits.groupByLimits(cqlRowLimit, cqlPerPartitionLimit, pageSize.inEstimatedRows(ResultSet.estimatedRowSizeForAllColumns(this.table)), aggregationSpec)):(this.parameters.isDistinct?(cqlRowLimit == 2147483647?DataLimits.DISTINCT_NONE:DataLimits.distinctLimits(cqlRowLimit)):DataLimits.cqlLimits(cqlRowLimit, cqlPerPartitionLimit));
   }

   public int getLimit(QueryOptions options) {
      return this.getLimit(this.limit, options);
   }

   public int getPerPartitionLimit(QueryOptions options) {
      return this.getLimit(this.perPartitionLimit, options);
   }

   private int getLimit(Term limit, QueryOptions options) {
      int userLimit = 2147483647;
      if(limit != null) {
         ByteBuffer b = (ByteBuffer)RequestValidations.checkNotNull(limit.bindAndGet(options), "Invalid null value of limit");
         if(b != ByteBufferUtil.UNSET_BYTE_BUFFER) {
            try {
               Int32Type.instance.validate(b);
               userLimit = ((Integer)Int32Type.instance.compose(b)).intValue();
               RequestValidations.checkTrue(userLimit > 0, "LIMIT must be strictly positive");
            } catch (MarshalException var6) {
               throw new InvalidRequestException("Invalid limit value");
            }
         }
      }

      return userLimit;
   }

   private NavigableSet<Clustering> getRequestedRows(QueryOptions options) throws InvalidRequestException {
      assert !this.restrictions.isColumnRange();

      return this.restrictions.getClusteringColumns(options);
   }

   public RowFilter getRowFilter(QueryState state, QueryOptions options) {
      ColumnFamilyStore cfs = Keyspace.open(this.keyspace()).getColumnFamilyStore(this.columnFamily());
      SecondaryIndexManager secondaryIndexManager = cfs.indexManager;
      return this.restrictions.getRowFilter(secondaryIndexManager, state, options);
   }

   private Single<ResultSet> process(Flow<FlowablePartition> partitions, QueryOptions options, Selection.Selectors selectors, int nowInSec, int userLimit, AggregationSpecification aggregationSpec) {
      ResultSet.Builder result = ResultSet.makeBuilder(this.getResultMetadata(), selectors, aggregationSpec);
      return partitions.flatProcess((partition) -> {
         return this.processPartition(partition, options, result, nowInSec);
      }).mapToRxSingle((VOID) -> {
         return this.postQueryProcessing(result, userLimit);
      });
   }

   private ResultSet postQueryProcessing(ResultSet.Builder result, int userLimit) {
      ResultSet cqlRows = result.build();
      this.orderResults(cqlRows);
      cqlRows.trim(userLimit);
      return cqlRows;
   }

   public static ByteBuffer[] getComponents(TableMetadata metadata, DecoratedKey dk) {
      ByteBuffer key = dk.getKey();
      return metadata.partitionKeyType instanceof CompositeType?((CompositeType)metadata.partitionKeyType).split(key):new ByteBuffer[]{key};
   }

   private boolean returnStaticContentOnPartitionWithNoRows() {
      return this.queriesFullPartitions() || this.table.isStaticCompactTable();
   }

   <T extends ResultBuilder> Flow<T> processPartition(FlowablePartition partition, QueryOptions options, T result, int nowInSec) throws InvalidRequestException {
      ProtocolVersion protocolVersion = options.getProtocolVersion();
      ByteBuffer[] keyComponents = getComponents(this.table, partition.header().partitionKey);
      Flow var10000 = partition.content();
      result.getClass();
      return var10000.takeUntil(result::isCompleted).reduce(Boolean.valueOf(false), (hasContent, row) -> {
         result.newRow(partition.partitionKey(), row.clustering());
         Iterator var8 = this.selection.getColumns().iterator();

         while(var8.hasNext()) {
            ColumnMetadata def = (ColumnMetadata)var8.next();
            switch(null.$SwitchMap$org$apache$cassandra$schema$ColumnMetadata$Kind[def.kind.ordinal()]) {
            case 1:
               result.add(keyComponents[def.position()]);
               break;
            case 2:
               addValue(result, def, partition.staticRow(), nowInSec, protocolVersion);
               break;
            case 3:
               result.add(row.clustering().get(def.position()));
               break;
            case 4:
               addValue(result, def, row, nowInSec, protocolVersion);
            }
         }

         return Boolean.valueOf(true);
      }).map((hasContent) -> {
         if(!hasContent.booleanValue() && !result.isCompleted() && !partition.staticRow().isEmpty() && this.returnStaticContentOnPartitionWithNoRows()) {
            result.newRow(partition.partitionKey(), partition.staticRow().clustering());
            Iterator var7 = this.selection.getColumns().iterator();

            while(var7.hasNext()) {
               ColumnMetadata def = (ColumnMetadata)var7.next();
               switch(null.$SwitchMap$org$apache$cassandra$schema$ColumnMetadata$Kind[def.kind.ordinal()]) {
               case 1:
                  result.add(keyComponents[def.position()]);
                  break;
               case 2:
                  addValue(result, def, partition.staticRow(), nowInSec, protocolVersion);
                  break;
               default:
                  result.add((ByteBuffer)null);
               }
            }
         }

         return result;
      });
   }

   private boolean queriesFullPartitions() {
      return !this.restrictions.hasClusteringColumnsRestrictions() && !this.restrictions.hasRegularColumnsRestrictions();
   }

   private static void addValue(ResultBuilder result, ColumnMetadata def, Row row, int nowInSec, ProtocolVersion protocolVersion) {
      if(def.isComplex()) {
         assert def.type.isMultiCell();

         ComplexColumnData complexData = row.getComplexColumnData(def);
         if(complexData == null) {
            result.add((ByteBuffer)null);
         } else {
            result.add(((MultiCellType)def.type).serializeForNativeProtocol(complexData.iterator(), protocolVersion));
         }
      } else {
         result.add(row.getCell(def), nowInSec);
      }

   }

   private boolean needsPostQueryOrdering() {
      return this.restrictions.keyIsInRelation() && !this.parameters.orderings.isEmpty();
   }

   private void orderResults(ResultSet cqlRows) {
      if(cqlRows.size() != 0 && this.needsPostQueryOrdering()) {
         Collections.sort(cqlRows.rows, this.orderingComparator);
      }
   }

   private static class CompositeComparator extends SelectStatement.ColumnComparator<List<ByteBuffer>> {
      private final List<Comparator<ByteBuffer>> orderTypes;
      private final List<Integer> positions;

      private CompositeComparator(List<Comparator<ByteBuffer>> orderTypes, List<Integer> positions) {
         super(null);
         this.orderTypes = orderTypes;
         this.positions = positions;
      }

      public int compare(List<ByteBuffer> a, List<ByteBuffer> b) {
         for(int i = 0; i < this.positions.size(); ++i) {
            Comparator<ByteBuffer> type = (Comparator)this.orderTypes.get(i);
            int columnPos = ((Integer)this.positions.get(i)).intValue();
            int comparison = this.compare(type, (ByteBuffer)a.get(columnPos), (ByteBuffer)b.get(columnPos));
            if(comparison != 0) {
               return comparison;
            }
         }

         return 0;
      }
   }

   private static class SingleColumnComparator extends SelectStatement.ColumnComparator<List<ByteBuffer>> {
      private final int index;
      private final Comparator<ByteBuffer> comparator;

      public SingleColumnComparator(int columnIndex, Comparator<ByteBuffer> orderer) {
         super(null);
         this.index = columnIndex;
         this.comparator = orderer;
      }

      public int compare(List<ByteBuffer> a, List<ByteBuffer> b) {
         return this.compare(this.comparator, (ByteBuffer)a.get(this.index), (ByteBuffer)b.get(this.index));
      }
   }

   private abstract static class ColumnComparator<T> implements Comparator<T> {
      private ColumnComparator() {
      }

      protected final int compare(Comparator<ByteBuffer> comparator, ByteBuffer aValue, ByteBuffer bValue) {
         return aValue == null?(bValue == null?0:-1):(bValue == null?1:comparator.compare(aValue, bValue));
      }
   }

   public static class Parameters {
      public final Map<ColumnMetadata.Raw, Boolean> orderings;
      public final List<Selectable.Raw> groups;
      public final boolean isDistinct;
      public final boolean allowFiltering;
      public final boolean isJson;

      public Parameters(Map<ColumnMetadata.Raw, Boolean> orderings, List<Selectable.Raw> groups, boolean isDistinct, boolean allowFiltering, boolean isJson) {
         this.orderings = orderings;
         this.groups = groups;
         this.isDistinct = isDistinct;
         this.allowFiltering = allowFiltering;
         this.isJson = isJson;
      }
   }

   public static class RawStatement extends CFStatement {
      public final SelectStatement.Parameters parameters;
      public final List<RawSelector> selectClause;
      public final WhereClause whereClause;
      public final Term.Raw limit;
      public final Term.Raw perPartitionLimit;

      public RawStatement(CFName cfName, SelectStatement.Parameters parameters, List<RawSelector> selectClause, WhereClause whereClause, Term.Raw limit, Term.Raw perPartitionLimit) {
         super(cfName);
         this.parameters = parameters;
         this.selectClause = selectClause;
         this.whereClause = whereClause;
         this.limit = limit;
         this.perPartitionLimit = perPartitionLimit;
      }

      public void prepareKeyspace(ClientState state) throws InvalidRequestException {
         super.prepareKeyspace(state);
      }

      public ParsedStatement.Prepared prepare() throws InvalidRequestException {
         return this.prepare(false);
      }

      public ParsedStatement.Prepared prepare(boolean forView) throws InvalidRequestException {
         TableMetadata table = Schema.instance.validateTable(this.keyspace(), this.columnFamily());
         VariableSpecifications boundNames = this.getBoundVariables();
         List<Selectable> selectables = RawSelector.toSelectables(this.selectClause, table);
         boolean containsOnlyStaticColumns = this.selectOnlyStaticColumns(table, selectables);
         StatementRestrictions restrictions = this.prepareRestrictions(table, boundNames, containsOnlyStaticColumns, forView);
         Map<ColumnMetadata, Boolean> orderingColumns = this.getOrderingColumns(table);
         Set<ColumnMetadata> resultSetOrderingColumns = restrictions.keyIsInRelation()?orderingColumns.keySet():Collections.emptySet();
         Selection selection = this.prepareSelection(table, selectables, boundNames, resultSetOrderingColumns, restrictions);
         if(this.parameters.isDistinct) {
            RequestValidations.checkNull(this.perPartitionLimit, "PER PARTITION LIMIT is not allowed with SELECT DISTINCT queries");
            validateDistinctSelection(table, selection, restrictions);
         }

         AggregationSpecification.Factory aggregationSpecFactory = this.getAggregationSpecFactory(table, boundNames, selection, restrictions, this.parameters.isDistinct);
         RequestValidations.checkFalse(aggregationSpecFactory == AggregationSpecification.AGGREGATE_EVERYTHING_FACTORY && this.perPartitionLimit != null, "PER PARTITION LIMIT is not allowed with aggregate queries.");
         Comparator<List<ByteBuffer>> orderingComparator = null;
         boolean isReversed = false;
         if(!orderingColumns.isEmpty()) {
            assert !forView;

            this.verifyOrderingIsAllowed(restrictions);
            orderingComparator = this.getOrderingComparator(selection, restrictions, orderingColumns);
            isReversed = this.isReversed(table, orderingColumns, restrictions);
            if(isReversed) {
               orderingComparator = Collections.reverseOrder(orderingComparator);
            }
         }

         this.checkNeedsFiltering(restrictions);
         SelectStatement stmt = new SelectStatement(table, boundNames.size(), this.parameters, selection, restrictions, isReversed, aggregationSpecFactory, orderingComparator, this.prepareLimit(boundNames, this.limit, this.keyspace(), this.limitReceiver()), this.prepareLimit(boundNames, this.perPartitionLimit, this.keyspace(), this.perPartitionLimitReceiver()));
         return new ParsedStatement.Prepared(stmt, boundNames, boundNames.getPartitionKeyBindIndexes(table));
      }

      private Selection prepareSelection(TableMetadata table, List<Selectable> selectables, VariableSpecifications boundNames, Set<ColumnMetadata> resultSetOrderingColumns, StatementRestrictions restrictions) {
         boolean hasGroupBy = !this.parameters.groups.isEmpty();
         return selectables.isEmpty()?(hasGroupBy?Selection.wildcardWithGroupBy(table, boundNames, this.parameters.isJson):Selection.wildcard(table, this.parameters.isJson)):Selection.fromSelectors(table, selectables, boundNames, resultSetOrderingColumns, restrictions.nonPKRestrictedColumns(false), hasGroupBy, this.parameters.isJson);
      }

      private boolean selectOnlyStaticColumns(TableMetadata table, List<Selectable> selectables) {
         return !table.isStaticCompactTable() && table.hasStaticColumns() && !selectables.isEmpty()?Selectable.selectColumns(selectables, (column) -> {
            return column.isStatic();
         }) && !Selectable.selectColumns(selectables, (column) -> {
            return !column.isPartitionKey() && !column.isStatic();
         }):false;
      }

      private Map<ColumnMetadata, Boolean> getOrderingColumns(TableMetadata table) {
         if(this.parameters.orderings.isEmpty()) {
            return Collections.emptyMap();
         } else {
            Map<ColumnMetadata, Boolean> orderingColumns = new LinkedHashMap();
            Iterator var3 = this.parameters.orderings.entrySet().iterator();

            while(var3.hasNext()) {
               Entry<ColumnMetadata.Raw, Boolean> entry = (Entry)var3.next();
               orderingColumns.put(((ColumnMetadata.Raw)entry.getKey()).prepare(table), entry.getValue());
            }

            return orderingColumns;
         }
      }

      protected StatementRestrictions prepareRestrictions(TableMetadata metadata, VariableSpecifications boundNames, boolean selectsOnlyStaticColumns, boolean forView) throws InvalidRequestException {
         return new StatementRestrictions(StatementType.SELECT, metadata, this.whereClause, boundNames, selectsOnlyStaticColumns, this.parameters.allowFiltering, forView);
      }

      private Term prepareLimit(VariableSpecifications boundNames, Term.Raw limit, String keyspace, ColumnSpecification limitReceiver) throws InvalidRequestException {
         if(limit == null) {
            return null;
         } else {
            Term prepLimit = limit.prepare(keyspace, limitReceiver);
            prepLimit.collectMarkerSpecification(boundNames);
            return prepLimit;
         }
      }

      protected void verifyOrderingIsAllowed(StatementRestrictions restrictions) throws InvalidRequestException {
         RequestValidations.checkFalse(restrictions.usesSecondaryIndexing(), "ORDER BY with 2ndary indexes is not supported.");
         RequestValidations.checkFalse(restrictions.isKeyRange(), "ORDER BY is only supported when the partition key is restricted by an EQ or an IN.");
      }

      private static void validateDistinctSelection(TableMetadata metadata, Selection selection, StatementRestrictions restrictions) throws InvalidRequestException {
         RequestValidations.checkFalse(restrictions.hasClusteringColumnsRestrictions() || restrictions.hasNonPrimaryKeyRestrictions() && !restrictions.nonPKRestrictedColumns(true).stream().allMatch(ColumnMetadata::isStatic), "SELECT DISTINCT with WHERE clause only supports restriction by partition key and/or static columns.");
         Collection<ColumnMetadata> requestedColumns = selection.getColumns();
         Iterator var4 = requestedColumns.iterator();

         ColumnMetadata def;
         while(var4.hasNext()) {
            def = (ColumnMetadata)var4.next();
            RequestValidations.checkFalse(!def.isPartitionKey() && !def.isStatic(), "SELECT DISTINCT queries must only request partition key columns and/or static columns (not %s)", def.name);
         }

         if(restrictions.isKeyRange()) {
            UnmodifiableIterator var6 = metadata.partitionKeyColumns().iterator();

            while(var6.hasNext()) {
               def = (ColumnMetadata)var6.next();
               RequestValidations.checkTrue(requestedColumns.contains(def), "SELECT DISTINCT queries must request all the partition key columns (missing %s)", def.name);
            }

         }
      }

      private AggregationSpecification.Factory getAggregationSpecFactory(TableMetadata metadata, VariableSpecifications boundNames, Selection selection, StatementRestrictions restrictions, boolean isDistinct) {
         if(this.parameters.groups.isEmpty()) {
            return selection.isAggregate()?AggregationSpecification.AGGREGATE_EVERYTHING_FACTORY:null;
         } else {
            int clusteringPrefixSize = 0;
            Iterator<ColumnMetadata> pkColumns = metadata.primaryKeyColumns().iterator();
            Selector.Factory selectorFactory = null;
            Iterator var9 = this.parameters.groups.iterator();

            while(var9.hasNext()) {
               Selectable.Raw raw = (Selectable.Raw)var9.next();
               RequestValidations.checkNull(selectorFactory, "Functions are only supported on the last element of the GROUP BY clause");
               Selectable selectable = raw.prepare(metadata);
               ColumnMetadata def = null;
               if(selectable instanceof Selectable.WithFunction) {
                  Selectable.WithFunction withFunction = (Selectable.WithFunction)selectable;
                  this.validateGroupByFunction(withFunction);
                  List<ColumnMetadata> columns = new ArrayList();
                  selectorFactory = selectable.newSelectorFactory(metadata, (AbstractType)null, columns, boundNames);
                  RequestValidations.checkFalse(columns.isEmpty(), "GROUP BY functions must have one clustering column name as parameter");
                  if(columns.size() > 1) {
                     throw RequestValidations.invalidRequest("GROUP BY functions accept only one clustering column as parameter, got: %s", new Object[]{columns.stream().map((c) -> {
                        return c.name.toCQLString();
                     }).collect(Collectors.joining(","))});
                  }

                  def = (ColumnMetadata)columns.get(0);
                  RequestValidations.checkTrue(def.isClusteringColumn(), "Group by functions are only supported on clustering columns, got %s", def.name);
               } else {
                  def = (ColumnMetadata)selectable;
                  RequestValidations.checkTrue(def.isPartitionKey() || def.isClusteringColumn(), "Group by is currently only supported on the columns of the PRIMARY KEY, got %s", def.name);
               }

               while(true) {
                  RequestValidations.checkTrue(pkColumns.hasNext(), "Group by currently only support groups of columns following their declared order in the PRIMARY KEY");
                  ColumnMetadata pkColumn = (ColumnMetadata)pkColumns.next();
                  if(pkColumn.isClusteringColumn()) {
                     ++clusteringPrefixSize;
                  }

                  if(pkColumn.equals(def)) {
                     break;
                  }

                  RequestValidations.checkTrue(restrictions.isColumnRestrictedByEq(pkColumn), "Group by currently only support groups of columns following their declared order in the PRIMARY KEY");
               }
            }

            RequestValidations.checkFalse(pkColumns.hasNext() && ((ColumnMetadata)pkColumns.next()).isPartitionKey(), "Group by is not supported on only a part of the partition key");
            RequestValidations.checkFalse(clusteringPrefixSize > 0 && isDistinct, "Grouping on clustering columns is not allowed for SELECT DISTINCT queries");
            return selectorFactory == null?AggregationSpecification.aggregatePkPrefixFactory(metadata.comparator, clusteringPrefixSize):AggregationSpecification.aggregatePkPrefixFactoryWithSelector(metadata.comparator, clusteringPrefixSize, selectorFactory);
         }
      }

      private void validateGroupByFunction(Selectable.WithFunction withFunction) {
         Function f = withFunction.getFunction();
         RequestValidations.checkFalse(f.isAggregate(), "Aggregate functions are not supported within the GROUP BY clause, got: %s", f.name());
      }

      protected Comparator<List<ByteBuffer>> getOrderingComparator(Selection selection, StatementRestrictions restrictions, Map<ColumnMetadata, Boolean> orderingColumns) {
         if(!restrictions.keyIsInRelation()) {
            return null;
         } else {
            List<Integer> idToSort = new ArrayList(orderingColumns.size());
            List<Comparator<ByteBuffer>> sorters = new ArrayList(orderingColumns.size());
            Iterator var6 = orderingColumns.keySet().iterator();

            while(var6.hasNext()) {
               ColumnMetadata orderingColumn = (ColumnMetadata)var6.next();
               idToSort.add(selection.getOrderingIndex(orderingColumn));
               sorters.add(orderingColumn.type);
            }

            return (Comparator)(idToSort.size() == 1?new SelectStatement.SingleColumnComparator(((Integer)idToSort.get(0)).intValue(), (Comparator)sorters.get(0)):new SelectStatement.CompositeComparator(sorters, idToSort, null));
         }
      }

      protected boolean isReversed(TableMetadata table, Map<ColumnMetadata, Boolean> orderingColumns, StatementRestrictions restrictions) throws InvalidRequestException {
         Boolean[] reversedMap = new Boolean[table.clusteringColumns().size()];
         int i = 0;

         ColumnMetadata def;
         boolean reversed;
         for(Iterator var6 = orderingColumns.entrySet().iterator(); var6.hasNext(); reversedMap[def.position()] = Boolean.valueOf(reversed != def.isReversedType())) {
            Entry<ColumnMetadata, Boolean> entry = (Entry)var6.next();
            def = (ColumnMetadata)entry.getKey();
            reversed = ((Boolean)entry.getValue()).booleanValue();
            RequestValidations.checkTrue(def.isClusteringColumn(), "Order by is currently only supported on the clustered columns of the PRIMARY KEY, got %s", def.name);

            while(i != def.position()) {
               RequestValidations.checkTrue(restrictions.isColumnRestrictedByEq((ColumnMetadata)table.clusteringColumns().get(i++)), "Order by currently only supports the ordering of columns following their declared order in the PRIMARY KEY");
            }

            ++i;
         }

         Boolean isReversed = null;
         Boolean[] var12 = reversedMap;
         int var13 = reversedMap.length;

         for(int var14 = 0; var14 < var13; ++var14) {
            Boolean b = var12[var14];
            if(b != null) {
               if(isReversed == null) {
                  isReversed = b;
               } else {
                  RequestValidations.checkTrue(isReversed.equals(b), "Unsupported order by relation");
               }
            }
         }

         assert isReversed != null;

         return isReversed.booleanValue();
      }

      private void checkNeedsFiltering(StatementRestrictions restrictions) throws InvalidRequestException {
         if(!this.parameters.allowFiltering && (restrictions.isKeyRange() || restrictions.usesSecondaryIndexing())) {
            RequestValidations.checkFalse(restrictions.needFiltering(), "Cannot execute this query as it might involve data filtering and thus may have unpredictable performance. If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING");
         }

      }

      private ColumnSpecification limitReceiver() {
         return new ColumnSpecification(this.keyspace(), this.columnFamily(), new ColumnIdentifier("[limit]", true), Int32Type.instance);
      }

      private ColumnSpecification perPartitionLimitReceiver() {
         return new ColumnSpecification(this.keyspace(), this.columnFamily(), new ColumnIdentifier("[per_partition_limit]", true), Int32Type.instance);
      }

      public String toString() {
         return MoreObjects.toStringHelper(this).add("name", this.cfName).add("selectClause", this.selectClause).add("whereClause", this.whereClause).add("isDistinct", this.parameters.isDistinct).toString();
      }
   }

   public static final class ContinuousPagingExecutor implements org.apache.cassandra.cql3.continuous.paging.ContinuousPagingExecutor {
      final SelectStatement statement;
      final QueryOptions options;
      final ReadContext.Builder paramsBuilder;
      final PageSize pageSize;
      final ReadQuery query;
      final boolean isLocalQuery;
      final long queryStartNanos;
      int coreId;
      SelectStatement.Pager pager;
      long schedulingTimeNanos;
      long taskStartMillis;

      private ContinuousPagingExecutor(SelectStatement statement, QueryOptions options, QueryState state, ReadQuery query, long queryStartNanos, PageSize pageSize) {
         this.statement = statement;
         this.options = options;
         this.paramsBuilder = ReadContext.builder(query, options.getConsistency()).state(state.getClientState()).forContinuousPaging();
         this.pageSize = pageSize;
         this.query = query;
         this.isLocalQuery = options.getConsistency().isSingleNode() && query.queriesOnlyLocalData();
         this.queryStartNanos = queryStartNanos;
         this.coreId = -1;
      }

      public PagingState state(boolean inclusive) {
         return this.pager != null && !this.pager.isExhausted()?this.pager.state(inclusive):null;
      }

      public boolean isLocalQuery() {
         return this.isLocalQuery;
      }

      public long queryStartTimeInNanos() {
         return this.queryStartNanos;
      }

      public long localStartTimeInMillis() {
         return this.isLocalQuery?this.taskStartMillis:-1L;
      }

      void retrieveMultiplePages(PagingState pagingState, ResultBuilder builder) {
         try {
            this.taskStartMillis = System.currentTimeMillis();
            ContinuousPagingService.metrics.waitingTime.addNano(System.nanoTime() - this.schedulingTimeNanos);
            if(SelectStatement.logger.isTraceEnabled()) {
               SelectStatement.logger.trace("{} - retrieving multiple pages with paging state {}", this.statement.table, pagingState);
            }

            assert this.pager == null;

            assert !builder.isCompleted();

            this.pager = SelectStatement.Pager.forNormalQuery(this.statement.getPager(this.query, pagingState, this.options.getProtocolVersion()), this.paramsBuilder);
            PageSize pageSize = this.isLocalQuery?PageSize.rowsSize(this.pager.maxRemaining()):this.pageSize;
            long queryStart = this.isLocalQuery?this.queryStartNanos:System.nanoTime();
            Flow<FlowablePartition> page = this.pager.fetchPage(pageSize, queryStart);
            builder.getClass();
            page.takeUntil(builder::isCompleted).flatMap((partition) -> {
               return this.statement.processPartition(partition, this.options, builder, this.query.nowInSec());
            }).reduceToFuture(builder, (b, v) -> {
               return b;
            }).whenComplete((bldr, error) -> {
               if(error == null) {
                  this.maybeReschedule(bldr);
               } else {
                  this.handleError(error, builder);
               }

            });
         } catch (Throwable var7) {
            JVMStabilityInspector.inspectThrowable(var7);
            if(var7 instanceof CassandraException) {
               SelectStatement.logger.debug("Failed to execute continuous paging session {}", builder, var7);
            } else {
               SelectStatement.logger.error("Failed to execute continuous paging session {}", builder, var7);
            }

            builder.complete(var7);
         }

      }

      private void handleError(Throwable error, ResultBuilder builder) {
         if(error instanceof ContinuousBackPressureException) {
            if(SelectStatement.logger.isTraceEnabled()) {
               SelectStatement.logger.trace("{} paused: {}", builder, error.getMessage());
            }

            assert !builder.isCompleted() : "session should not have been paused if already completed";
         } else if(!(error instanceof ReadFailureException) && !(error instanceof ReadTimeoutException)) {
            JVMStabilityInspector.inspectThrowable(error);
            SelectStatement.logger.error("{} failed with unexpected error: {}", new Object[]{builder, error.getMessage(), error});
            builder.complete(error);
         } else {
            SelectStatement.logger.debug("Continuous paging query failed: {}", error.getMessage());
            builder.complete(error);
         }

      }

      void maybeReschedule(ResultBuilder builder) {
         assert this.pager != null;

         if(this.pager.isExhausted()) {
            builder.complete();
         } else if(!builder.isCompleted()) {
            this.schedule(this.pager.state(false), builder);
         }

      }

      public void schedule(Runnable runnable, long delay, TimeUnit unit) {
         TPCScheduler scheduler = TPC.getForCore(this.coreId);
         if(delay == 0L) {
            scheduler.enqueue(TPCRunnable.wrap(runnable, TPCTaskType.CONTINUOUS_PAGING, this.coreId));
         } else {
            scheduler.schedule(runnable, TPCTaskType.CONTINUOUS_PAGING, delay, unit);
         }

      }

      public void schedule(PagingState pagingState, ResultBuilder builder) {
         try {
            if(SelectStatement.logger.isTraceEnabled()) {
               SelectStatement.logger.trace("{} - scheduling retrieving of multiple pages with paging state {}", this.statement.table, pagingState);
            }

            this.pager = null;
            this.schedulingTimeNanos = System.nanoTime();
            this.schedule(() -> {
               this.retrieveMultiplePages(pagingState, builder);
            });
         } catch (Throwable var4) {
            JVMStabilityInspector.inspectThrowable(var4);
            SelectStatement.logger.error("Failed to schedule continuous paging session {}", builder, var4);
            builder.complete(var4);
         }

      }

      public int coreId() {
         return this.coreId;
      }

      public void setCoreId(int coreId) {
         this.coreId = coreId;
      }
   }

   private abstract static class Pager {
      protected QueryPager pager;

      protected Pager(QueryPager pager) {
         this.pager = pager;
      }

      public static SelectStatement.Pager forInternalQuery(QueryPager pager) {
         return new SelectStatement.Pager.InternalPager(pager, null);
      }

      public static SelectStatement.Pager forNormalQuery(QueryPager pager, ReadContext.Builder paramsBuilder) {
         return new SelectStatement.Pager.NormalPager(pager, paramsBuilder, null);
      }

      public boolean isExhausted() {
         return this.pager.isExhausted();
      }

      public PagingState state(boolean inclusive) {
         return this.pager.state(inclusive);
      }

      public int maxRemaining() {
         return this.pager.maxRemaining();
      }

      public abstract Flow<FlowablePartition> fetchPage(PageSize var1, long var2);

      public static class InternalPager extends SelectStatement.Pager {
         private InternalPager(QueryPager pager) {
            super(pager);
         }

         public Flow<FlowablePartition> fetchPage(PageSize pageSize, long queryStartNanoTime) {
            return this.pager.fetchPageInternal(pageSize);
         }
      }

      public static class NormalPager extends SelectStatement.Pager {
         private final ReadContext.Builder paramsBuilder;

         private NormalPager(QueryPager pager, ReadContext.Builder paramsBuilder) {
            super(pager);
            this.paramsBuilder = paramsBuilder;
         }

         public Flow<FlowablePartition> fetchPage(PageSize pageSize, long queryStartNanoTime) {
            return this.pager.fetchPage(pageSize, this.paramsBuilder.build(queryStartNanoTime));
         }
      }
   }
}
