package org.apache.cassandra.db;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.reactivex.functions.Action;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Predicate;
import io.reactivex.schedulers.Schedulers;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.function.Function;
import org.apache.cassandra.cache.IRowCacheEntry;
import org.apache.cassandra.cache.RowCacheKey;
import org.apache.cassandra.cache.RowCacheSentinel;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCRunnable;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.concurrent.TracingAwareExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.monitoring.Monitor;
import org.apache.cassandra.db.partitions.ArrayBackedPartition;
import org.apache.cassandra.db.partitions.CachedBTreePartition;
import org.apache.cassandra.db.partitions.CachedPartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.db.rows.FlowablePartitions;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.io.sstable.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.net.Request;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.ReadRepairDecision;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.pager.MultiPartitionPager;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.service.pager.SinglePartitionPager;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.TopKSampler;
import org.apache.cassandra.utils.btree.BTreeSet;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.flow.Threads;
import org.apache.cassandra.utils.versioning.Versioned;

public class SinglePartitionReadCommand extends ReadCommand {
   private static final ReadCommand.SelectionDeserializer<SinglePartitionReadCommand> selectionDeserializer = new SinglePartitionReadCommand.Deserializer();
   public static final Versioned<ReadVerbs.ReadVersion, Serializer<SinglePartitionReadCommand>> serializers = ReadVerbs.ReadVersion.versioned((v) -> {
      return new ReadCommand.ReadCommandSerializer(v, selectionDeserializer);
   });
   private final DecoratedKey partitionKey;
   private final ClusteringIndexFilter clusteringIndexFilter;
   private final transient StagedScheduler scheduler;
   private final transient TracingAwareExecutor requestExecutor;
   private final transient TracingAwareExecutor responseExecutor;
   private final transient TPCTaskType readType;
   private int oldestUnrepairedTombstone;

   private SinglePartitionReadCommand(DigestVersion digestVersion, TableMetadata metadata, int nowInSec, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits, DecoratedKey partitionKey, ClusteringIndexFilter clusteringIndexFilter, IndexMetadata index, TPCTaskType readType) {
      this(digestVersion, metadata, nowInSec, columnFilter, rowFilter, limits, partitionKey, clusteringIndexFilter, index, readType, (StagedScheduler)TPC.bestTPCScheduler());
   }

   private SinglePartitionReadCommand(DigestVersion digestVersion, TableMetadata metadata, int nowInSec, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits, DecoratedKey partitionKey, ClusteringIndexFilter clusteringIndexFilter, IndexMetadata index, TPCTaskType readType, StagedScheduler scheduler) {
      super(digestVersion, metadata, nowInSec, columnFilter, rowFilter, limits, index, readType);
      this.oldestUnrepairedTombstone = 2147483647;

      assert partitionKey.getPartitioner() == metadata.partitioner;

      this.partitionKey = partitionKey;
      this.clusteringIndexFilter = clusteringIndexFilter;
      if(SchemaConstants.isInternalKeyspace(metadata.keyspace)) {
         readType = TPCTaskType.READ_INTERNAL;
      }

      this.scheduler = scheduler;
      this.requestExecutor = scheduler.forTaskType(readType);
      this.responseExecutor = scheduler.forTaskType(TPCTaskType.READ_RESPONSE);
      this.readType = readType;
   }

   public Request.Dispatcher<SinglePartitionReadCommand, ReadResponse> dispatcherTo(Collection<InetAddress> endpoints) {
      return Verbs.READS.SINGLE_READ.newDispatcher(endpoints, this);
   }

   public Request<SinglePartitionReadCommand, ReadResponse> requestTo(InetAddress endpoint) {
      return Verbs.READS.SINGLE_READ.newRequest(endpoint, this);
   }

   public static SinglePartitionReadCommand create(TableMetadata metadata, int nowInSec, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits, DecoratedKey partitionKey, ClusteringIndexFilter clusteringIndexFilter, IndexMetadata indexMetadata, StagedScheduler scheduler) {
      return new SinglePartitionReadCommand((DigestVersion)null, metadata, nowInSec, columnFilter, rowFilter, limits, partitionKey, clusteringIndexFilter, indexMetadata, TPCTaskType.READ_LOCAL, scheduler);
   }

   public static SinglePartitionReadCommand create(TableMetadata metadata, int nowInSec, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits, DecoratedKey partitionKey, ClusteringIndexFilter clusteringIndexFilter, IndexMetadata indexMetadata) {
      return new SinglePartitionReadCommand((DigestVersion)null, metadata, nowInSec, columnFilter, rowFilter, limits, partitionKey, clusteringIndexFilter, indexMetadata, TPCTaskType.READ_LOCAL);
   }

   public static SinglePartitionReadCommand create(TableMetadata metadata, int nowInSec, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits, DecoratedKey partitionKey, ClusteringIndexFilter clusteringIndexFilter, TPCTaskType taskType) {
      return new SinglePartitionReadCommand((DigestVersion)null, metadata, nowInSec, columnFilter, rowFilter, limits, partitionKey, clusteringIndexFilter, findIndex(metadata, rowFilter), taskType);
   }

   public static SinglePartitionReadCommand create(TableMetadata metadata, int nowInSec, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits, DecoratedKey partitionKey, ClusteringIndexFilter clusteringIndexFilter) {
      return create(metadata, nowInSec, columnFilter, rowFilter, limits, partitionKey, clusteringIndexFilter, findIndex(metadata, rowFilter));
   }

   public static SinglePartitionReadCommand create(TableMetadata metadata, int nowInSec, DecoratedKey key, ColumnFilter columnFilter, ClusteringIndexFilter filter) {
      return create(metadata, nowInSec, columnFilter, RowFilter.NONE, DataLimits.NONE, key, filter);
   }

   public static SinglePartitionReadCommand createForIndex(TableMetadata metadata, int nowInSec, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits, DecoratedKey partitionKey, ClusteringIndexFilter clusteringIndexFilter) {
      return new SinglePartitionReadCommand((DigestVersion)null, metadata, nowInSec, columnFilter, rowFilter, limits, partitionKey, clusteringIndexFilter, (IndexMetadata)null, TPCTaskType.READ_SECONDARY_INDEX);
   }

   public static SinglePartitionReadCommand fullPartitionRead(TableMetadata metadata, int nowInSec, DecoratedKey key) {
      return create(metadata, nowInSec, key, Slices.ALL);
   }

   public static SinglePartitionReadCommand fullPartitionRead(TableMetadata metadata, int nowInSec, ByteBuffer key) {
      return create(metadata, nowInSec, metadata.partitioner.decorateKey(key), Slices.ALL);
   }

   public static SinglePartitionReadCommand create(TableMetadata metadata, int nowInSec, DecoratedKey key, Slice slice) {
      return create(metadata, nowInSec, key, Slices.with(metadata.comparator, slice));
   }

   public static SinglePartitionReadCommand create(TableMetadata metadata, int nowInSec, DecoratedKey key, Slices slices) {
      ClusteringIndexSliceFilter filter = new ClusteringIndexSliceFilter(slices, false);
      return create(metadata, nowInSec, ColumnFilter.all(metadata), RowFilter.NONE, DataLimits.NONE, key, filter);
   }

   public static SinglePartitionReadCommand create(TableMetadata metadata, int nowInSec, ByteBuffer key, Slices slices) {
      return create(metadata, nowInSec, metadata.partitioner.decorateKey(key), slices);
   }

   public static SinglePartitionReadCommand create(TableMetadata metadata, int nowInSec, DecoratedKey key, NavigableSet<Clustering> names) {
      ClusteringIndexNamesFilter filter = new ClusteringIndexNamesFilter(names, false);
      return create(metadata, nowInSec, ColumnFilter.all(metadata), RowFilter.NONE, DataLimits.NONE, key, filter);
   }

   public static SinglePartitionReadCommand create(TableMetadata metadata, int nowInSec, DecoratedKey key, Clustering name) {
      return create(metadata, nowInSec, key, FBUtilities.singleton(name, metadata.comparator));
   }

   public SinglePartitionReadCommand createDigestCommand(DigestVersion digestVersion) {
      return new SinglePartitionReadCommand(digestVersion, this.metadata(), this.nowInSec(), this.columnFilter(), this.rowFilter(), this.limits(), this.partitionKey(), this.clusteringIndexFilter(), this.indexMetadata(), TPCTaskType.READ_LOCAL);
   }

   public DecoratedKey partitionKey() {
      return this.partitionKey;
   }

   public ClusteringIndexFilter clusteringIndexFilter() {
      return this.clusteringIndexFilter;
   }

   public ClusteringIndexFilter clusteringIndexFilter(DecoratedKey key) {
      return this.clusteringIndexFilter;
   }

   public long getTimeout() {
      return DatabaseDescriptor.getReadRpcTimeout();
   }

   public boolean isReversed() {
      return this.clusteringIndexFilter.isReversed();
   }

   public boolean selectsKey(DecoratedKey key) {
      return !this.partitionKey().equals(key)?false:this.rowFilter().partitionKeyRestrictionsAreSatisfiedBy(key, this.metadata().partitionKeyType);
   }

   public boolean selectsClustering(DecoratedKey key, Clustering clustering) {
      return clustering == Clustering.STATIC_CLUSTERING?!this.columnFilter().fetchedColumns().statics.isEmpty():(!this.clusteringIndexFilter().selects(clustering)?false:this.rowFilter().clusteringKeyRestrictionsAreSatisfiedBy(clustering));
   }

   public SinglePartitionReadCommand forPaging(Clustering lastReturned, DataLimits limits, boolean inclusive) {
      assert !this.isDigestQuery();

      return create(this.metadata(), this.nowInSec(), this.columnFilter(), this.rowFilter(), limits, this.partitionKey(), lastReturned == null?this.clusteringIndexFilter():this.clusteringIndexFilter.forPaging(this.metadata().comparator, lastReturned, inclusive));
   }

   public SinglePartitionReadCommand withUpdatedLimit(DataLimits newLimits) {
      return new SinglePartitionReadCommand(this.digestVersion(), this.metadata(), this.nowInSec(), this.columnFilter(), this.rowFilter(), newLimits, this.partitionKey(), this.clusteringIndexFilter(), this.indexMetadata(), TPCTaskType.READ_LOCAL);
   }

   public Flow<FlowablePartition> execute(ReadContext ctx) throws RequestExecutionException {
      return StorageProxy.read(SinglePartitionReadCommand.Group.one(this), ctx);
   }

   public SinglePartitionPager getPager(PagingState pagingState, ProtocolVersion protocolVersion) {
      return getPager(this, pagingState, protocolVersion);
   }

   private static SinglePartitionPager getPager(SinglePartitionReadCommand command, PagingState pagingState, ProtocolVersion protocolVersion) {
      return new SinglePartitionPager(command, pagingState, protocolVersion);
   }

   protected void recordLatency(TableMetrics metric, long latencyNanos) {
      metric.readLatency.addNano(latencyNanos);
   }

   public Flow<FlowableUnfilteredPartition> queryStorage(ColumnFamilyStore cfs, ReadExecutionController executionController) {
      return cfs.isRowCacheEnabled()?this.getThroughCache(cfs, executionController):this.deferredQuery(cfs, executionController);
   }

   public Flow<FlowableUnfilteredPartition> deferredQuery(ColumnFamilyStore cfs, ReadExecutionController executionController) {
      SinglePartitionReadCommand.SSTableReadMetricsCollector metricsCollector = new SinglePartitionReadCommand.SSTableReadMetricsCollector();
      int localCore = TPC.getCoreId();
      int coreId = TPC.getNextCore();
      boolean isLocalCore = coreId == localCore;
      return !isLocalCore && this.readType != TPCTaskType.READ_INTERNAL && this.readType != TPCTaskType.READ_REMOTE && this.readType != TPCTaskType.READ_SECONDARY_INDEX?Threads.deferOnCore(() -> {
         return this.queryMemtableAndDisk(cfs, executionController, metricsCollector).doOnClose(() -> {
            this.updateMetrics(cfs.metric, metricsCollector);
         });
      }, coreId, TPCTaskType.READ_DEFERRED):this.queryMemtableAndDisk(cfs, executionController, metricsCollector).doOnClose(() -> {
         this.updateMetrics(cfs.metric, metricsCollector);
      });
   }

   private void updateMetrics(TableMetrics metrics, SinglePartitionReadCommand.SSTableReadMetricsCollector metricsCollector) {
      int mergedSSTablesIterated = metricsCollector.getMergedSSTables();
      metrics.updateSSTableIterated(mergedSSTablesIterated);
      Tracing.trace("Merged data from memtables and {} sstables", (Object)Integer.valueOf(mergedSSTablesIterated));
   }

   private Flow<FlowableUnfilteredPartition> getThroughCache(ColumnFamilyStore cfs, ReadExecutionController executionController) {
      assert !cfs.isIndex();

      assert cfs.isRowCacheEnabled() : String.format("Row cache is not enabled on table [%s]", new Object[]{cfs.name});

      RowCacheKey key = new RowCacheKey(this.metadata(), this.partitionKey());
      IRowCacheEntry cached = (IRowCacheEntry)CacheService.instance.rowCache.get(key);
      boolean started;
      if(cached != null) {
         if(cached instanceof RowCacheSentinel) {
            Tracing.trace("Row cache miss (race)");
            cfs.metric.rowCacheMiss.inc();
            return this.deferredQuery(cfs, executionController);
         } else {
            CachedPartition cachedPartition = (CachedPartition)cached;
            if(cfs.isFilterFullyCoveredBy(this.clusteringIndexFilter(), this.limits(), cachedPartition, this.nowInSec(), this.metadata().enforceStrictLiveness())) {
               cfs.metric.rowCacheHit.inc();
               Tracing.trace("Row cache hit");
               FlowableUnfilteredPartition ret = this.clusteringIndexFilter().getFlowableUnfilteredPartition(this.columnFilter(), cachedPartition);
               cfs.metric.updateSSTableIterated(0);
               started = executionController.startIfValid(cfs);

               assert started;

               return Flow.just(ret);
            } else {
               cfs.metric.rowCacheHitOutOfRange.inc();
               Tracing.trace("Ignoring row cache as cached value could not satisfy query");
               return this.deferredQuery(cfs, executionController);
            }
         }
      } else {
         cfs.metric.rowCacheMiss.inc();
         Tracing.trace("Row cache miss");
         boolean cacheFullPartitions = this.metadata().clusteringColumns().size() > 0?this.metadata().params.caching.cacheAllRows():this.metadata().params.caching.cacheRows();
         if(!cacheFullPartitions && !this.clusteringIndexFilter().isHeadFilter()) {
            Tracing.trace("Fetching data but not populating cache as query does not query from the start of the partition");
         } else {
            RowCacheSentinel sentinel = new RowCacheSentinel();
            started = CacheService.instance.rowCache.putIfAbsent(key, sentinel);
            if(started) {
               int rowsToCache = this.metadata().params.caching.rowsPerPartitionToCache();
               boolean enforceStrictLiveness = this.metadata().enforceStrictLiveness();
               Flow<FlowableUnfilteredPartition> iter = fullPartitionRead(this.metadata(), this.nowInSec(), this.partitionKey()).deferredQuery(cfs, executionController);
               return iter.flatMap((partition) -> {
                  Flow.Tee<Unfiltered> tee = partition.content().tee();
                  FlowableUnfilteredPartition toCache = partition.withContent(tee.child(0));
                  FlowableUnfilteredPartition toReturn = partition.withContent(tee.child(1));
                  toCache = DataLimits.cqlLimits(rowsToCache).truncateUnfiltered(toCache, this.nowInSec(), false, enforceStrictLiveness);
                  Flow<CachedBTreePartition> cachedPartition = CachedBTreePartition.create(toCache, this.nowInSec());
                  cachedPartition.doOnError((error) -> {
                     cfs.invalidateCachedPartition(key);
                  }).reduceToFuture((Object)null, (VOID, c) -> {
                     if(!c.isEmpty()) {
                        Tracing.trace("Caching {} rows", (Object)Integer.valueOf(c.rowCount()));
                        CacheService.instance.rowCache.replace(key, sentinel, c);
                     } else {
                        cfs.invalidateCachedPartition(key);
                     }

                     return null;
                  });
                  return Flow.just(this.clusteringIndexFilter().filterNotIndexed(this.columnFilter(), toReturn));
               });
            }
         }

         return this.deferredQuery(cfs, executionController);
      }
   }

   private Flow<FlowableUnfilteredPartition> queryMemtableAndDisk(ColumnFamilyStore cfs, ReadExecutionController executionController, SinglePartitionReadCommand.SSTableReadMetricsCollector metricsCollector) {
      boolean started = executionController.startIfValid(cfs);

      assert started;

      Tracing.trace("Executing single-partition query on {}", (Object)cfs.name);
      return this.clusteringIndexFilter().kind() == ClusteringIndexFilter.Kind.NAMES && !this.queriesMulticellType(cfs.metadata())?this.queryMemtableAndSSTablesInTimestampOrder(cfs, (ClusteringIndexNamesFilter)this.clusteringIndexFilter(), metricsCollector):this.queryMemtableAndDiskInternal(cfs, metricsCollector);
   }

   protected int oldestUnrepairedTombstone() {
      return this.oldestUnrepairedTombstone;
   }

   private Flow<FlowableUnfilteredPartition> queryMemtableAndDiskInternal(ColumnFamilyStore cfs, SinglePartitionReadCommand.SSTableReadMetricsCollector metricsCollector) {
      Tracing.trace("Acquiring sstable references");
      ColumnFamilyStore.ViewFragment view = cfs.select(View.select(SSTableSet.LIVE, this.partitionKey()));
      List<Flow<FlowableUnfilteredPartition>> iterators = new ArrayList(3);
      SinglePartitionReadCommand.MutableState state = new SinglePartitionReadCommand.MutableState();
      iterators.add(Flow.fromIterable(view.memtables).flatMap((memtable) -> {
         state.minTimestamp = Math.min(state.minTimestamp, memtable.getMinTimestamp());
         return memtable.getPartition(this.partitionKey());
      }).skippingMap((p) -> {
         if(p == null) {
            return null;
         } else {
            this.oldestUnrepairedTombstone = Math.min(this.oldestUnrepairedTombstone, p.stats().minLocalDeletionTime);
            state.mostRecentPartitionTombstone = Math.max(state.mostRecentPartitionTombstone, p.partitionLevelDeletion().markedForDeleteAt());
            return this.clusteringIndexFilter().getFlowableUnfilteredPartition(this.columnFilter(), p);
         }
      }));
      Collections.sort(view.sstables, SSTableReader.maxTimestampComparator);
      List<SSTableReader> skippedSSTablesWithTombstones = null;
      List<SSTableReader> filteredSSTables = new ArrayList(view.sstables.size());
      Iterator var8 = view.sstables.iterator();

      while(var8.hasNext()) {
         SSTableReader sstable = (SSTableReader)var8.next();
         ++state.allTableCount;
         if(!this.shouldInclude(sstable)) {
            ++state.nonIntersectingSSTables;
            if(sstable.mayHaveTombstones()) {
               if(skippedSSTablesWithTombstones == null) {
                  skippedSSTablesWithTombstones = new ArrayList();
               }

               skippedSSTablesWithTombstones.add(sstable);
            }
         } else {
            filteredSSTables.add(sstable);
         }
      }

      iterators.add(Flow.fromIterable(filteredSSTables).takeWhile((sstable) -> {
         return sstable.getMaxTimestamp() >= state.mostRecentPartitionTombstone;
      }).flatMap((sstable) -> {
         state.minTimestamp = Math.min(state.minTimestamp, sstable.getMinTimestamp());
         if(!sstable.isRepaired()) {
            this.oldestUnrepairedTombstone = Math.min(this.oldestUnrepairedTombstone, sstable.getMinLocalDeletionTime());
         }

         return this.makeFlowableWithLowerBound(sstable, metricsCollector);
      }).map((fup) -> {
         state.mostRecentPartitionTombstone = Math.max(state.mostRecentPartitionTombstone, fup.header().partitionLevelDeletion.markedForDeleteAt());
         return fup;
      }));
      if(skippedSSTablesWithTombstones != null) {
         iterators.add(Flow.fromIterable(skippedSSTablesWithTombstones).takeWhile((sstable) -> {
            return sstable.getMaxTimestamp() > state.minTimestamp;
         }).flatMap((sstable) -> {
            ++state.includedDueToTombstones;
            if(!sstable.isRepaired()) {
               this.oldestUnrepairedTombstone = Math.min(this.oldestUnrepairedTombstone, sstable.getMinLocalDeletionTime());
            }

            return this.makeFlowableWithLowerBound(sstable, metricsCollector);
         }));
      }

      return Flow.concat((Iterable)iterators).toList().map((sources) -> {
         return this.mergeResult(cfs, sources, state.nonIntersectingSSTables, state.allTableCount, state.includedDueToTombstones);
      });
   }

   private FlowableUnfilteredPartition mergeResult(ColumnFamilyStore cfs, List<FlowableUnfilteredPartition> fups, int nonIntersectingSSTables, int allTableCount, int includedDueToTombstones) {
      if(Tracing.isTracing()) {
         Tracing.trace("Skipped {}/{} non-slice-intersecting sstables, included {} due to tombstones", new Object[]{Integer.valueOf(nonIntersectingSSTables), Integer.valueOf(allTableCount), Integer.valueOf(includedDueToTombstones)});
      }

      if(fups.isEmpty()) {
         return FlowablePartitions.empty(this.metadata(), this.partitionKey(), this.clusteringIndexFilter().isReversed());
      } else {
         StorageHook.instance.reportRead(this.metadata().id, this.partitionKey());
         ((TopKSampler)cfs.metric.samplers.get(TableMetrics.Sampler.READS)).addSample(this.partitionKey.getKey(), (long)this.partitionKey.hashCode(), 1);
         return FlowablePartitions.merge(fups, this.nowInSec(), (UnfilteredRowIterators.MergeListener)null);
      }
   }

   private boolean shouldInclude(SSTableReader sstable) {
      return !this.columnFilter().fetchedColumns().statics.isEmpty()?true:this.clusteringIndexFilter().shouldInclude(sstable);
   }

   private Flow<FlowableUnfilteredPartition> makeFlowable(SSTableReader sstable, ClusteringIndexNamesFilter clusterFilter, SinglePartitionReadCommand.SSTableReadMetricsCollector metricsCollector) {
      return sstable.flow(this.partitionKey(), clusterFilter.getSlices(this.metadata()), this.columnFilter(), this.isReversed(), metricsCollector);
   }

   private Flow<FlowableUnfilteredPartition> makeFlowableWithLowerBound(SSTableReader sstable, SinglePartitionReadCommand.SSTableReadMetricsCollector metricsCollector) {
      return sstable.flowWithLowerBound(this.partitionKey(), this.clusteringIndexFilter().getSlices(this.metadata()), this.columnFilter(), this.isReversed(), metricsCollector);
   }

   private boolean queriesMulticellType(TableMetadata table) {
      if(!table.hasMulticellOrCounterColumn) {
         return false;
      } else {
         Iterator var2 = this.columnFilter().fetchedColumns().iterator();

         ColumnMetadata column;
         do {
            if(!var2.hasNext()) {
               return false;
            }

            column = (ColumnMetadata)var2.next();
         } while(!column.type.isMultiCell() && !column.type.isCounter());

         return true;
      }
   }

   private Flow<FlowableUnfilteredPartition> queryMemtableAndSSTablesInTimestampOrder(ColumnFamilyStore cfs, ClusteringIndexNamesFilter initFilter, SinglePartitionReadCommand.SSTableReadMetricsCollector metricsCollector) {
      Tracing.trace("Acquiring sstable references");
      ColumnFamilyStore.ViewFragment view = cfs.select(View.select(SSTableSet.LIVE, this.partitionKey()));
      SinglePartitionReadCommand.MutableState state = new SinglePartitionReadCommand.MutableState();
      state.namesFilter = initFilter;
      Flow<FlowableUnfilteredPartition> pipeline = Flow.fromIterable(view.memtables).flatMap((memtable) -> {
         return memtable.getPartition(this.partitionKey());
      }).skippingMap((p) -> {
         if(p == null) {
            return null;
         } else {
            this.oldestUnrepairedTombstone = Math.min(this.oldestUnrepairedTombstone, p.stats().minLocalDeletionTime);
            return state.namesFilter.getFlowableUnfilteredPartition(this.columnFilter(), p);
         }
      });
      if(!view.sstables.isEmpty()) {
         Collections.sort(view.sstables, SSTableReader.maxTimestampComparator);
         Flow<FlowableUnfilteredPartition> sstablePipeline = Flow.fromIterable(view.sstables).takeWhile((sstable) -> {
            if(state.timeOrderedResults != null && sstable.getMaxTimestamp() < state.timeOrderedResults.partitionLevelDeletion().markedForDeleteAt()) {
               return false;
            } else {
               long currentMaxTs = sstable.getMaxTimestamp();
               state.namesFilter = this.reduceFilter(state.namesFilter, state.timeOrderedResults, currentMaxTs);
               return state.namesFilter != null;
            }
         }).flatMap((sstable) -> {
            if(!this.shouldInclude(sstable) && !sstable.mayHaveTombstones()) {
               return Flow.empty();
            } else {
               Tracing.trace("Merging data from sstable {}", (Object)Integer.valueOf(sstable.descriptor.generation));
               if(sstable.isRepaired()) {
                  state.onlyUnrepaired = false;
               } else {
                  this.oldestUnrepairedTombstone = Math.min(this.oldestUnrepairedTombstone, sstable.getMinLocalDeletionTime());
               }

               return this.makeFlowable(sstable, state.namesFilter, metricsCollector);
            }
         });
         pipeline = Flow.concat(new Flow[]{pipeline, sstablePipeline});
      }

      return pipeline.flatMap((p) -> {
         return this.mergeToMemory(state.timeOrderedResults, p, initFilter);
      }).process((p) -> {
         state.timeOrderedResults = p;
      }).map((v) -> {
         return this.outputTimeOrderedResult(cfs, state.timeOrderedResults, metricsCollector, state.onlyUnrepaired);
      });
   }

   private FlowableUnfilteredPartition outputTimeOrderedResult(ColumnFamilyStore cfs, Partition timeOrderedResult, SinglePartitionReadCommand.SSTableReadMetricsCollector metricsCollector, boolean onlyUnrepaired) {
      if(timeOrderedResult != null && !timeOrderedResult.isEmpty()) {
         DecoratedKey key = timeOrderedResult.partitionKey();
         ((TopKSampler)cfs.metric.samplers.get(TableMetrics.Sampler.READS)).addSample(key.getKey(), (long)key.hashCode(), 1);
         StorageHook.instance.reportRead(cfs.metadata.id, this.partitionKey());
         if(metricsCollector.getMergedSSTables() > cfs.getMinimumCompactionThreshold() && onlyUnrepaired && !cfs.isAutoCompactionDisabled() && cfs.getCompactionStrategyManager().shouldDefragment()) {
            Tracing.trace("Defragmenting requested data");
            Schedulers.io().scheduleDirect(new TPCRunnable(() -> {
               UnfilteredRowIterator iter = timeOrderedResult.unfilteredIterator(this.columnFilter(), Slices.ALL, false);
               Throwable var3 = null;

               try {
                  Mutation mutation = new Mutation(PartitionUpdate.fromIterator(iter, this.columnFilter()));
                  Keyspace.open(mutation.getKeyspaceName()).apply(mutation, false, false, true).subscribe();
               } catch (Throwable var12) {
                  var3 = var12;
                  throw var12;
               } finally {
                  if(iter != null) {
                     if(var3 != null) {
                        try {
                           iter.close();
                        } catch (Throwable var11) {
                           var3.addSuppressed(var11);
                        }
                     } else {
                        iter.close();
                     }
                  }

               }

            }, ExecutorLocals.create(), TPCTaskType.WRITE_DEFRAGMENT, TPC.getNumCores()));
         }

         return timeOrderedResult.unfilteredPartition(this.columnFilter(), Slices.ALL, this.clusteringIndexFilter().isReversed());
      } else {
         return FlowablePartitions.empty(this.metadata(), this.partitionKey(), this.clusteringIndexFilter().isReversed());
      }
   }

   private Flow<Partition> mergeToMemory(Partition prev, FlowableUnfilteredPartition partition, ClusteringIndexNamesFilter filter) {
      int maxRows = Math.max(filter.requestedRows().size(), 1);
      FlowableUnfilteredPartition merged;
      if(prev != null && !prev.isEmpty()) {
         merged = FlowablePartitions.merge(ImmutableList.of(partition, prev.unfilteredPartition(this.columnFilter(), Slices.ALL, filter.isReversed())), this.nowInSec(), (UnfilteredRowIterators.MergeListener)null);
      } else {
         merged = partition;
      }

      return ArrayBackedPartition.create(merged, maxRows);
   }

   private ClusteringIndexNamesFilter reduceFilter(ClusteringIndexNamesFilter filter, Partition result, long sstableTimestamp) {
      if(result == null) {
         return filter;
      } else {
         SearchIterator<Clustering, Row> searchIter = result.searchIterator(this.columnFilter(), false);
         RegularAndStaticColumns columns = this.columnFilter().fetchedColumns();
         NavigableSet<Clustering> clusterings = filter.requestedRows();
         boolean removeStatic = false;
         if(!columns.statics.isEmpty()) {
            Row staticRow = (Row)searchIter.next(Clustering.STATIC_CLUSTERING);
            removeStatic = staticRow != null && this.canRemoveRow(staticRow, columns.statics, sstableTimestamp);
         }

         NavigableSet<Clustering> toRemove = null;
         Iterator var10 = ((NavigableSet)clusterings).iterator();

         while(var10.hasNext()) {
            Clustering clustering = (Clustering)var10.next();
            Row row = (Row)searchIter.next(clustering);
            if(row != null && this.canRemoveRow(row, columns.regulars, sstableTimestamp)) {
               if(toRemove == null) {
                  toRemove = new TreeSet(result.metadata().comparator);
               }

               toRemove.add(clustering);
            }
         }

         if(!removeStatic && toRemove == null) {
            return filter;
         } else {
            boolean hasNoMoreStatic = columns.statics.isEmpty() || removeStatic;
            boolean hasNoMoreClusterings = ((NavigableSet)clusterings).isEmpty() || toRemove != null && toRemove.size() == ((NavigableSet)clusterings).size();
            if(hasNoMoreStatic && hasNoMoreClusterings) {
               return null;
            } else {
               if(toRemove != null) {
                  BTreeSet.Builder<Clustering> newClusterings = BTreeSet.builder(result.metadata().comparator);
                  newClusterings.addAll(Sets.difference((Set)clusterings, toRemove));
                  clusterings = newClusterings.build();
               }

               return new ClusteringIndexNamesFilter((NavigableSet)clusterings, filter.isReversed());
            }
         }
      }
   }

   private boolean canRemoveRow(Row row, Columns requestedColumns, long sstableTimestamp) {
      if(!row.primaryKeyLivenessInfo().isEmpty() && row.primaryKeyLivenessInfo().timestamp() > sstableTimestamp) {
         Iterator var5 = requestedColumns.iterator();

         Cell cell;
         do {
            if(!var5.hasNext()) {
               return true;
            }

            ColumnMetadata column = (ColumnMetadata)var5.next();
            cell = row.getCell(column);
         } while(cell != null && cell.timestamp() > sstableTimestamp);

         return false;
      } else {
         return false;
      }
   }

   public boolean queriesOnlyLocalData() {
      return StorageProxy.isLocalToken(this.metadata().keyspace, this.partitionKey.getToken());
   }

   public boolean selectsFullPartition() {
      return this.metadata().isStaticCompactTable() || this.clusteringIndexFilter.selectsAllPartition() && !this.rowFilter().hasExpressionOnClusteringOrRegularColumns();
   }

   public ReadContext.Builder applyDefaults(ReadContext.Builder ctx) {
      return ctx.useDigests().readRepairDecision(ReadRepairDecision.newDecision(this.metadata()));
   }

   public String toString() {
      return String.format("Read(%s columns=%s rowFilter=%s limits=%s key=%s filter=%s, nowInSec=%d)", new Object[]{this.metadata().toString(), this.columnFilter(), this.rowFilter(), this.limits(), this.metadata().partitionKeyType.getString(this.partitionKey().getKey()), this.clusteringIndexFilter.toString(this.metadata()), Integer.valueOf(this.nowInSec())});
   }

   protected void appendCQLWhereClause(StringBuilder sb) {
      sb.append(" WHERE ");
      sb.append(ColumnMetadata.toCQLString((Iterable)this.metadata().partitionKeyColumns())).append(" = ");
      DataRange.appendKeyString(sb, this.metadata().partitionKeyType, this.partitionKey().getKey());
      if(!this.rowFilter().isEmpty()) {
         sb.append(" AND ").append(this.rowFilter());
      }

      String filterString = this.clusteringIndexFilter().toCQLString(this.metadata());
      if(!filterString.isEmpty()) {
         sb.append(" AND ").append(filterString);
      }

   }

   protected void serializeSelection(DataOutputPlus out, ReadVerbs.ReadVersion version) throws IOException {
      this.metadata().partitionKeyType.writeValue(this.partitionKey().getKey(), out);
      ((ClusteringIndexFilter.Serializer)ClusteringIndexFilter.serializers.get(version)).serialize(this.clusteringIndexFilter(), out);
   }

   protected long selectionSerializedSize(ReadVerbs.ReadVersion version) {
      return (long)this.metadata().partitionKeyType.writtenLength(this.partitionKey().getKey()) + ((ClusteringIndexFilter.Serializer)ClusteringIndexFilter.serializers.get(version)).serializedSize(this.clusteringIndexFilter());
   }

   public boolean isLimitedToOnePartition() {
      return true;
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
         SinglePartitionReadCommand that = (SinglePartitionReadCommand)other;
         return this.partitionKey.equals(that.partitionKey) && this.clusteringIndexFilter.equals(that.clusteringIndexFilter);
      }
   }

   private static final class SSTableReadMetricsCollector implements SSTableReadsListener {
      private int mergedSSTables;

      private SSTableReadMetricsCollector() {
      }

      public void onSSTableSelected(SSTableReader sstable, RowIndexEntry indexEntry, SSTableReadsListener.SelectionReason reason) {
         sstable.incrementReadCount();
         ++this.mergedSSTables;
      }

      private int getMergedSSTables() {
         return this.mergedSSTables;
      }
   }

   private static class Deserializer extends ReadCommand.SelectionDeserializer<SinglePartitionReadCommand> {
      private Deserializer() {
      }

      public SinglePartitionReadCommand deserialize(DataInputPlus in, ReadVerbs.ReadVersion version, DigestVersion digestVersion, TableMetadata metadata, int nowInSec, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits, IndexMetadata index) throws IOException {
         DecoratedKey key = metadata.partitioner.decorateKey(metadata.partitionKeyType.readValue(in, DatabaseDescriptor.getMaxValueSize()));
         ClusteringIndexFilter filter = ((ClusteringIndexFilter.Serializer)ClusteringIndexFilter.serializers.get(version)).deserialize(in, metadata);
         return new SinglePartitionReadCommand(digestVersion, metadata, nowInSec, columnFilter, rowFilter, limits, key, filter, index, TPCTaskType.READ_REMOTE);
      }
   }

   public static class Group implements ReadQuery {
      public final List<SinglePartitionReadCommand> commands;
      private final DataLimits limits;
      private final int nowInSec;
      private final boolean selectsFullPartitions;
      private final boolean enforceStrictLiveness;

      public Group(List<SinglePartitionReadCommand> commands, DataLimits limits) {
         assert !commands.isEmpty();

         this.commands = commands;
         this.limits = limits;
         SinglePartitionReadCommand firstCommand = (SinglePartitionReadCommand)commands.get(0);
         this.nowInSec = firstCommand.nowInSec();
         this.selectsFullPartitions = firstCommand.selectsFullPartition();
         this.enforceStrictLiveness = firstCommand.metadata().enforceStrictLiveness();

         for(int i = 1; i < commands.size(); ++i) {
            assert ((SinglePartitionReadCommand)commands.get(i)).nowInSec() == this.nowInSec;
         }

      }

      public static SinglePartitionReadCommand.Group one(SinglePartitionReadCommand command) {
         return new SinglePartitionReadCommand.Group(Collections.singletonList(command), command.limits());
      }

      public Flow<FlowablePartition> execute(ReadContext ctx) throws RequestExecutionException {
         return StorageProxy.read(this, ctx);
      }

      public int nowInSec() {
         return this.nowInSec;
      }

      public DataLimits limits() {
         return this.limits;
      }

      public TableMetadata metadata() {
         return ((SinglePartitionReadCommand)this.commands.get(0)).metadata();
      }

      public boolean isEmpty() {
         return false;
      }

      public ReadContext.Builder applyDefaults(ReadContext.Builder ctx) {
         return ((SinglePartitionReadCommand)this.commands.get(0)).applyDefaults(ctx);
      }

      public boolean selectsFullPartition() {
         return this.selectsFullPartitions;
      }

      public ReadExecutionController executionController() {
         return ((SinglePartitionReadCommand)this.commands.get(0)).executionController();
      }

      public Flow<FlowablePartition> executeInternal(Monitor monitor) {
         return this.limits.truncateFiltered(FlowablePartitions.filterAndSkipEmpty(this.executeLocally(monitor, false), this.nowInSec()), this.nowInSec(), this.selectsFullPartitions, this.enforceStrictLiveness);
      }

      public Flow<FlowableUnfilteredPartition> executeLocally(Monitor monitor) {
         return this.executeLocally(monitor, true);
      }

      private Flow<FlowableUnfilteredPartition> executeLocally(Monitor monitor, boolean sort) {
         if(this.commands.size() == 1) {
            return ((SinglePartitionReadCommand)this.commands.get(0)).executeLocally(monitor);
         } else {
            List<SinglePartitionReadCommand> commands = this.commands;
            if(sort) {
               commands = new ArrayList((Collection)commands);
               ((List)commands).sort(Comparator.comparing(SinglePartitionReadCommand::partitionKey));
            }

            return Flow.fromIterable((Iterable)commands).flatMap((command) -> {
               return command.executeLocally(monitor);
            });
         }
      }

      public QueryPager getPager(PagingState pagingState, ProtocolVersion protocolVersion) {
         return (QueryPager)(this.commands.size() == 1?SinglePartitionReadCommand.getPager((SinglePartitionReadCommand)this.commands.get(0), pagingState, protocolVersion):new MultiPartitionPager(this, pagingState, protocolVersion));
      }

      public boolean selectsKey(DecoratedKey key) {
         return Iterables.any(this.commands, (c) -> {
            return c.selectsKey(key);
         });
      }

      public boolean selectsClustering(DecoratedKey key, Clustering clustering) {
         return Iterables.any(this.commands, (c) -> {
            return c.selectsClustering(key, clustering);
         });
      }

      public boolean queriesOnlyLocalData() {
         Iterator var1 = this.commands.iterator();

         SinglePartitionReadCommand cmd;
         do {
            if(!var1.hasNext()) {
               return true;
            }

            cmd = (SinglePartitionReadCommand)var1.next();
         } while(cmd.queriesOnlyLocalData());

         return false;
      }

      public String toCQLString() {
         return Joiner.on("; ").join(Iterables.transform(this.commands, ReadCommand::toCQLString));
      }

      public String toString() {
         return this.commands.toString();
      }
   }

   class MutableState {
      long mostRecentPartitionTombstone = -9223372036854775808L;
      long minTimestamp = -9223372036854775808L;
      int allTableCount = 0;
      int includedDueToTombstones = 0;
      int nonIntersectingSSTables = 0;
      boolean onlyUnrepaired = true;
      Partition timeOrderedResults = null;
      ClusteringIndexNamesFilter namesFilter = null;

      MutableState() {
      }
   }
}
