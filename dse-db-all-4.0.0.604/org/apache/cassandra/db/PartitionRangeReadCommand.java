package org.apache.cassandra.db;

import com.google.common.collect.Iterables;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.concurrent.TracingAwareExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.partitions.CachedPartition;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.db.rows.FlowablePartitions;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.net.Request;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.service.pager.PartitionRangeQueryPager;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.versioning.Versioned;

public class PartitionRangeReadCommand extends ReadCommand {
   private static final ReadCommand.SelectionDeserializer<PartitionRangeReadCommand> selectionDeserializer = new PartitionRangeReadCommand.Deserializer();
   public static final Versioned<ReadVerbs.ReadVersion, Serializer<PartitionRangeReadCommand>> serializers = ReadVerbs.ReadVersion.versioned((v) -> {
      return new ReadCommand.ReadCommandSerializer(v, selectionDeserializer);
   });
   private final DataRange dataRange;
   private int oldestUnrepairedTombstone = 2147483647;
   private final transient StagedScheduler scheduler;
   private final transient TracingAwareExecutor requestExecutor;
   private final transient TracingAwareExecutor responseExecutor;

   protected PartitionRangeReadCommand(DigestVersion digestVersion, TableMetadata metadata, int nowInSec, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits, DataRange dataRange, IndexMetadata index, StagedScheduler scheduler, TPCTaskType readType) {
      super(digestVersion, metadata, nowInSec, columnFilter, rowFilter, limits, index, readType);
      this.dataRange = dataRange;
      if(SchemaConstants.isInternalKeyspace(metadata.keyspace)) {
         readType = TPCTaskType.READ_RANGE_INTERNAL;
      }

      this.scheduler = (StagedScheduler)(scheduler == null?TPC.getNextTPCScheduler():scheduler);
      this.requestExecutor = this.scheduler.forTaskType(readType);
      this.responseExecutor = this.scheduler.forTaskType(TPCTaskType.READ_RANGE_RESPONSE);
   }

   protected PartitionRangeReadCommand copy(DigestVersion digestVersion, TableMetadata metadata, int nowInSec, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits, DataRange dataRange, IndexMetadata index, StagedScheduler scheduler) {
      return new PartitionRangeReadCommand(digestVersion, metadata, nowInSec, columnFilter, rowFilter, limits, dataRange, index, scheduler, this.readType);
   }

   public static PartitionRangeReadCommand create(TableMetadata metadata, int nowInSec, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits, DataRange dataRange, TPCTaskType readType) {
      return new PartitionRangeReadCommand((DigestVersion)null, metadata, nowInSec, columnFilter, rowFilter, limits, dataRange, findIndex(metadata, rowFilter), (StagedScheduler)null, readType);
   }

   public static PartitionRangeReadCommand create(TableMetadata metadata, int nowInSec, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits, DataRange dataRange) {
      return create(metadata, nowInSec, columnFilter, rowFilter, limits, dataRange, TPCTaskType.READ_RANGE_LOCAL);
   }

   public static PartitionRangeReadCommand allDataRead(TableMetadata metadata, int nowInSec) {
      return allDataRead(metadata, ColumnFilter.all(metadata), nowInSec);
   }

   public static PartitionRangeReadCommand allDataRead(TableMetadata metadata, ColumnFilter columnFilter, int nowInSec) {
      return new PartitionRangeReadCommand((DigestVersion)null, metadata, nowInSec, columnFilter, RowFilter.NONE, DataLimits.NONE, DataRange.allData(metadata.partitioner), (IndexMetadata)null, (StagedScheduler)null, TPCTaskType.READ_RANGE_LOCAL);
   }

   public static PartitionRangeReadCommand fullRangeRead(TableMetadata metadata, DataRange range, int nowInSec) {
      return new PartitionRangeReadCommand((DigestVersion)null, metadata, nowInSec, ColumnFilter.all(metadata), RowFilter.NONE, DataLimits.NONE, range, (IndexMetadata)null, (StagedScheduler)null, TPCTaskType.READ_RANGE_LOCAL);
   }

   public Request.Dispatcher<? extends PartitionRangeReadCommand, ReadResponse> dispatcherTo(Collection<InetAddress> endpoints) {
      return Verbs.READS.RANGE_READ.newDispatcher(endpoints, this);
   }

   public Request<? extends PartitionRangeReadCommand, ReadResponse> requestTo(InetAddress endpoint) {
      return Verbs.READS.RANGE_READ.newRequest(endpoint, this);
   }

   public DataRange dataRange() {
      return this.dataRange;
   }

   public ClusteringIndexFilter clusteringIndexFilter(DecoratedKey key) {
      return this.dataRange.clusteringIndexFilter(key);
   }

   public PartitionRangeReadCommand forSubRange(AbstractBounds<PartitionPosition> range, boolean isRangeContinuation) {
      DataRange newRange = this.dataRange().forSubRange(range);
      return this.copy(this.digestVersion(), this.metadata(), this.nowInSec(), this.columnFilter(), this.rowFilter(), isRangeContinuation?this.limits():this.limits().withoutState(), newRange, this.indexMetadata(), (StagedScheduler)null);
   }

   public PartitionRangeReadCommand createDigestCommand(DigestVersion digestVersion) {
      return this.copy(digestVersion, this.metadata(), this.nowInSec(), this.columnFilter(), this.rowFilter(), this.limits(), this.dataRange(), this.indexMetadata(), this.scheduler);
   }

   public PartitionRangeReadCommand withUpdatedLimit(DataLimits newLimits) {
      return this.copy(this.digestVersion(), this.metadata(), this.nowInSec(), this.columnFilter(), this.rowFilter(), newLimits, this.dataRange(), this.indexMetadata(), this.scheduler);
   }

   public PartitionRangeReadCommand withUpdatedLimitsAndDataRange(DataLimits newLimits, DataRange newDataRange) {
      return this.copy(this.digestVersion(), this.metadata(), this.nowInSec(), this.columnFilter(), this.rowFilter(), newLimits, newDataRange, this.indexMetadata(), (StagedScheduler)null);
   }

   public long getTimeout() {
      return DatabaseDescriptor.getRangeRpcTimeout();
   }

   public boolean isReversed() {
      return this.dataRange.isReversed();
   }

   public boolean selectsKey(DecoratedKey key) {
      return !this.dataRange().contains(key)?false:this.rowFilter().partitionKeyRestrictionsAreSatisfiedBy(key, this.metadata().partitionKeyType);
   }

   public boolean selectsClustering(DecoratedKey key, Clustering clustering) {
      return clustering == Clustering.STATIC_CLUSTERING?!this.columnFilter().fetchedColumns().statics.isEmpty():(!this.dataRange().clusteringIndexFilter(key).selects(clustering)?false:this.rowFilter().clusteringKeyRestrictionsAreSatisfiedBy(clustering));
   }

   public Flow<FlowablePartition> execute(ReadContext ctx) throws RequestExecutionException {
      return StorageProxy.getRangeSlice(this, ctx);
   }

   public QueryPager getPager(PagingState pagingState, ProtocolVersion protocolVersion) {
      return new PartitionRangeQueryPager(this, pagingState, protocolVersion);
   }

   protected void recordLatency(TableMetrics metric, long latencyNanos) {
      metric.rangeLatency.addNano(latencyNanos);
   }

   public Flow<FlowableUnfilteredPartition> queryStorage(ColumnFamilyStore cfs, ReadExecutionController executionController) {
      boolean started = executionController.startIfValid(cfs);

      assert started;

      ColumnFamilyStore.ViewFragment view = cfs.select(View.selectLive(this.dataRange().keyRange()));
      Tracing.trace("Executing seq scan across {} sstables for {}", Integer.valueOf(view.sstables.size()), this.dataRange().keyRange().getString(this.metadata().partitionKeyType));
      List<Flow<FlowableUnfilteredPartition>> iterators = new ArrayList(Iterables.size(view.memtables) + view.sstables.size());
      Iterator var6 = view.memtables.iterator();

      while(var6.hasNext()) {
         Memtable memtable = (Memtable)var6.next();
         Flow<FlowableUnfilteredPartition> iter = memtable.makePartitionFlow(this.columnFilter(), this.dataRange());
         this.oldestUnrepairedTombstone = Math.min(this.oldestUnrepairedTombstone, memtable.getMinLocalDeletionTime());
         iterators.add(iter);
      }

      SSTableReadsListener readCountUpdater = newReadCountUpdater();
      Iterator var10 = view.sstables.iterator();

      while(var10.hasNext()) {
         SSTableReader sstable = (SSTableReader)var10.next();
         iterators.add(sstable.getAsyncScanner(this.columnFilter(), this.dataRange(), readCountUpdater));
         if(!sstable.isRepaired()) {
            this.oldestUnrepairedTombstone = Math.min(this.oldestUnrepairedTombstone, sstable.getMinLocalDeletionTime());
         }
      }

      if(iterators.isEmpty()) {
         return Flow.empty();
      } else if(cfs.isRowCacheEnabled()) {
         return FlowablePartitions.mergePartitions(iterators, this.nowInSec(), (FlowablePartitions.MergeListener)null).map((partition) -> {
            DecoratedKey dk = partition.partitionKey();
            CachedPartition cached = cfs.getRawCachedPartition(dk);
            ClusteringIndexFilter filter = this.dataRange().clusteringIndexFilter(dk);
            return cached != null && cfs.isFilterFullyCoveredBy(filter, this.limits(), cached, this.nowInSec(), this.metadata().enforceStrictLiveness())?filter.getFlowableUnfilteredPartition(this.columnFilter(), cached):partition;
         });
      } else {
         return FlowablePartitions.mergePartitions(iterators, this.nowInSec(), (FlowablePartitions.MergeListener)null);
      }
   }

   private static SSTableReadsListener newReadCountUpdater() {
      return new SSTableReadsListener() {
         public void onScanningStarted(SSTableReader sstable) {
            sstable.incrementReadCount();
         }
      };
   }

   protected int oldestUnrepairedTombstone() {
      return this.oldestUnrepairedTombstone;
   }

   protected void appendCQLWhereClause(StringBuilder sb) {
      if(!this.dataRange.isUnrestricted() || !this.rowFilter().isEmpty()) {
         sb.append(" WHERE ");
         if(!this.rowFilter().isEmpty()) {
            sb.append(this.rowFilter());
            if(!this.dataRange.isUnrestricted()) {
               sb.append(" AND ");
            }
         }

         if(!this.dataRange.isUnrestricted()) {
            sb.append(this.dataRange.toCQLString(this.metadata()));
         }

      }
   }

   public Flow<FlowablePartition> withLimitsAndPostReconciliation(Flow<FlowablePartition> partitions) {
      return this.limits().truncateFiltered(this.postReconciliationProcessing(partitions), this.nowInSec(), this.selectsFullPartition(), this.metadata().enforceStrictLiveness());
   }

   public Flow<FlowablePartition> postReconciliationProcessing(Flow<FlowablePartition> partitions) {
      ColumnFamilyStore cfs = Keyspace.open(this.metadata().keyspace).getColumnFamilyStore(this.metadata().name);
      Index index = this.getIndex(cfs);
      return index == null?partitions:(Flow)index.postProcessorFor(this).apply(partitions, this);
   }

   public boolean queriesOnlyLocalData() {
      return StorageProxy.isLocalRange(this.metadata().keyspace, this.dataRange.keyRange());
   }

   public boolean selectsFullPartition() {
      return this.metadata().isStaticCompactTable() || this.dataRange.selectsAllPartition() && !this.rowFilter().hasExpressionOnClusteringOrRegularColumns();
   }

   public String toString() {
      return String.format("Read(%s columns=%s rowfilter=%s limits=%s %s)", new Object[]{this.metadata().toString(), this.columnFilter(), this.rowFilter(), this.limits(), this.dataRange().toString(this.metadata())});
   }

   protected void serializeSelection(DataOutputPlus out, ReadVerbs.ReadVersion version) throws IOException {
      ((DataRange.Serializer)DataRange.serializers.get(version)).serialize(this.dataRange(), out, this.metadata());
   }

   protected long selectionSerializedSize(ReadVerbs.ReadVersion version) {
      return ((DataRange.Serializer)DataRange.serializers.get(version)).serializedSize(this.dataRange(), this.metadata());
   }

   public boolean isLimitedToOnePartition() {
      return this.dataRange.keyRange instanceof Bounds && this.dataRange.startKey().kind() == PartitionPosition.Kind.ROW_KEY && this.dataRange.startKey().equals(this.dataRange.stopKey());
   }

   public StagedScheduler getScheduler() {
      return this.scheduler;
   }

   public TracingAwareExecutor getRequestExecutor() {
      return this.requestExecutor;
   }

   public TracingAwareExecutor getResponseExecutor() {
      return this.responseExecutor;
   }

   public boolean equals(Object other) {
      if(!this.isSame(other)) {
         return false;
      } else {
         PartitionRangeReadCommand that = (PartitionRangeReadCommand)other;
         return this.dataRange.equals(that.dataRange);
      }
   }

   private static class Deserializer extends ReadCommand.SelectionDeserializer<PartitionRangeReadCommand> {
      private Deserializer() {
      }

      public PartitionRangeReadCommand deserialize(DataInputPlus in, ReadVerbs.ReadVersion version, DigestVersion digestVersion, TableMetadata metadata, int nowInSec, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits, IndexMetadata index) throws IOException {
         DataRange range = ((DataRange.Serializer)DataRange.serializers.get(version)).deserialize(in, metadata);
         return new PartitionRangeReadCommand(digestVersion, metadata, nowInSec, columnFilter, rowFilter, limits, range, index, (StagedScheduler)null, TPCTaskType.READ_RANGE_REMOTE);
      }
   }
}
