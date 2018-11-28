package org.apache.cassandra.db;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import io.reactivex.Single;
import io.reactivex.functions.Function;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCBoundaries;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.partitions.AbstractBTreePartition;
import org.apache.cassandra.db.partitions.AtomicBTreePartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.FlowablePartitionBase;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IncludingExcludingBounds;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.FSDiskFullWriteError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Reducer;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.flow.FlowSource;
import org.apache.cassandra.utils.flow.FlowSubscriber;
import org.apache.cassandra.utils.flow.FlowSubscriptionRecipient;
import org.apache.cassandra.utils.flow.Threads;
import org.apache.cassandra.utils.memory.HeapPool;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.apache.cassandra.utils.memory.MemtablePool;
import org.apache.cassandra.utils.memory.NativePool;
import org.apache.cassandra.utils.memory.SlabPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Memtable implements Comparable<Memtable> {
   private static final Logger logger = LoggerFactory.getLogger(Memtable.class);
   public static final MemtablePool MEMORY_POOL = createMemtableAllocatorPool();
   private static final int ROW_OVERHEAD_HEAP_SIZE = estimateRowOverhead(Integer.parseInt(System.getProperty("cassandra.memtable_row_overhead_computation_step", "100000")));
   private volatile OpOrder.Barrier writeBarrier;
   private volatile AtomicReference<CommitLogPosition> commitLogUpperBound;
   private AtomicReference<CommitLogPosition> commitLogLowerBound;
   private final CommitLogPosition approximateCommitLogLowerBound;
   private final TPCBoundaries boundaries;
   private final MemtableSubrange[] subranges;
   public final ColumnFamilyStore cfs;
   public final TableMetadata metadata;
   private final long creationNano;
   public final ClusteringComparator initialComparator;

   private static MemtablePool createMemtableAllocatorPool() {
      long heapLimit = DatabaseDescriptor.getMemtableHeapSpaceInMb() << 20;
      long offHeapLimit = DatabaseDescriptor.getMemtableOffheapSpaceInMb() << 20;
      switch (DatabaseDescriptor.getMemtableAllocationType()) {
         case unslabbed_heap_buffers: {
            logger.debug("Memtables allocating with on heap buffers");
            return new HeapPool(heapLimit, DatabaseDescriptor.getMemtableCleanupThreshold(), new ColumnFamilyStore.FlushLargestColumnFamily());
         }
         case heap_buffers: {
            logger.debug("Memtables allocating with on heap slabs");
            return new SlabPool(heapLimit, 0L, DatabaseDescriptor.getMemtableCleanupThreshold(), new ColumnFamilyStore.FlushLargestColumnFamily());
         }
         case offheap_buffers: {
            if (!FileUtils.isCleanerAvailable) {
               throw new IllegalStateException("Could not free direct byte buffer: offheap_buffers is not a safe memtable_allocation_type without this ability, please adjust your config. This feature is only guaranteed to work on an Oracle JVM. Refusing to start.");
            }
            logger.debug("Memtables allocating with off heap buffers");
            return new SlabPool(heapLimit, offHeapLimit, DatabaseDescriptor.getMemtableCleanupThreshold(), new ColumnFamilyStore.FlushLargestColumnFamily());
         }
         case offheap_objects: {
            logger.debug("Memtables allocating with off heap objects");
            return new NativePool(heapLimit, offHeapLimit, DatabaseDescriptor.getMemtableCleanupThreshold(), new ColumnFamilyStore.FlushLargestColumnFamily());
         }
      }
      throw new AssertionError();
   }

   public int compareTo(Memtable that) {
      return this.approximateCommitLogLowerBound.compareTo(that.approximateCommitLogLowerBound);
   }

   public Memtable(AtomicReference<CommitLogPosition> commitLogLowerBound, ColumnFamilyStore cfs) {
      this.approximateCommitLogLowerBound = CommitLog.instance.getCurrentPosition();
      this.creationNano = System.nanoTime();
      this.cfs = cfs;
      this.commitLogLowerBound = commitLogLowerBound;
      this.initialComparator = cfs.metadata().comparator;
      this.metadata = cfs.metadata();
      this.cfs.scheduleFlush();
      this.boundaries = cfs.keyspace.getTPCBoundaries();
      this.subranges = this.generatePartitionSubranges(this.boundaries.supportedCores());
   }

   @VisibleForTesting
   public Memtable(TableMetadata metadata) {
      this.approximateCommitLogLowerBound = CommitLog.instance.getCurrentPosition();
      this.creationNano = System.nanoTime();
      this.initialComparator = metadata.comparator;
      this.metadata = metadata;
      this.cfs = null;
      this.boundaries = TPCBoundaries.NONE;
      this.subranges = this.generatePartitionSubranges(this.boundaries.supportedCores());
   }

   private int getCoreFor(DecoratedKey key) {
      int coreId = TPC.getCoreForKey(this.boundaries, key);

      assert coreId >= 0 && coreId < this.subranges.length : "Received invalid core id : " + coreId;

      return coreId;
   }

   @VisibleForTesting
   TPCBoundaries getBoundaries() {
      return this.boundaries;
   }

   public void allocateExtraOnHeap(long additionalSpace) {
      this.subranges[0].allocator().onHeap().allocated(additionalSpace);
   }

   public void allocateExtraOffHeap(long additionalSpace) {
      this.subranges[0].allocator().offHeap().allocated(additionalSpace);
   }

   public Memtable.MemoryUsage getMemoryUsage() {
      Memtable.MemoryUsage stats = new Memtable.MemoryUsage();
      this.addMemoryUsage(stats);
      return stats;
   }

   public void addMemoryUsage(Memtable.MemoryUsage stats) {
      MemtableSubrange[] var2 = this.subranges;
      int var3 = var2.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         MemtableSubrange s = var2[var4];
         stats.ownershipRatioOnHeap += s.allocator().onHeap().ownershipRatio();
         stats.ownershipRatioOffHeap += s.allocator().offHeap().ownershipRatio();
         stats.ownsOnHeap += s.allocator().onHeap().owns();
         stats.ownsOffHeap += s.allocator().offHeap().owns();
      }

   }

   @VisibleForTesting
   public void setDiscarding(OpOrder.Barrier writeBarrier, AtomicReference<CommitLogPosition> commitLogUpperBound) {
      assert this.writeBarrier == null;

      this.commitLogUpperBound = commitLogUpperBound;
      this.writeBarrier = writeBarrier;
      MemtableSubrange[] var3 = this.subranges;
      int var4 = var3.length;

      for(int var5 = 0; var5 < var4; ++var5) {
         MemtableSubrange sr = var3[var5];
         sr.allocator().setDiscarding();
      }

   }

   void setDiscarded() {
      MemtableSubrange[] var1 = this.subranges;
      int var2 = var1.length;

      for(int var3 = 0; var3 < var2; ++var3) {
         MemtableSubrange sr = var1[var3];
         sr.allocator().setDiscarded();
      }

   }

   public boolean accepts(OpOrder.Group opGroup, CommitLogPosition commitLogPosition) {
      OpOrder.Barrier barrier = this.writeBarrier;
      if(barrier == null) {
         return true;
      } else if(!barrier.isAfter(opGroup)) {
         return false;
      } else if(commitLogPosition == null) {
         return true;
      } else {
         CommitLogPosition currentLast;
         do {
            currentLast = (CommitLogPosition)this.commitLogUpperBound.get();
            if(currentLast instanceof Memtable.LastCommitLogPosition) {
               return currentLast.compareTo(commitLogPosition) >= 0;
            }

            if(currentLast != null && currentLast.compareTo(commitLogPosition) >= 0) {
               return true;
            }
         } while(!this.commitLogUpperBound.compareAndSet(currentLast, commitLogPosition));

         return true;
      }
   }

   public CommitLogPosition getCommitLogLowerBound() {
      return (CommitLogPosition)this.commitLogLowerBound.get();
   }

   public CommitLogPosition getCommitLogUpperBound() {
      return (CommitLogPosition)this.commitLogUpperBound.get();
   }

   public boolean isLive() {
      return this.subranges[0].allocator().isLive();
   }

   public boolean isClean() {
      return this.isEmpty() && this.boundaries.equals(this.cfs.keyspace.getTPCBoundaries());
   }

   public boolean isEmpty() {
      MemtableSubrange[] var1 = this.subranges;
      int var2 = var1.length;

      for(int var3 = 0; var3 < var2; ++var3) {
         MemtableSubrange memtableSubrange = var1[var3];
         if(!memtableSubrange.isEmpty()) {
            return false;
         }
      }

      return true;
   }

   public boolean mayContainDataBefore(CommitLogPosition position) {
      return this.approximateCommitLogLowerBound.compareTo(position) < 0;
   }

   public boolean isExpired() {
      int period = this.metadata.params.memtableFlushPeriodInMs;
      return period > 0 && System.nanoTime() - this.creationNano >= TimeUnit.MILLISECONDS.toNanos((long)period);
   }

   Single<Long> put(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup) {
      DecoratedKey key = update.partitionKey();
      int coreId = this.getCoreFor(key);
      MemtableSubrange partitionMap = this.subranges[coreId];
      Callable<Single<Long>> write = () -> {
         AtomicBTreePartition previous = partitionMap.get(key, MemtableSubrange.DataAccess.UNSAFE);

         assert TPC.getCoreId() == coreId;

         assert this.writeBarrier == null || this.writeBarrier.isAfter(opGroup) : String.format("Put called after write barrier\n%s", new Object[]{FBUtilities.Debug.getStackTrace()});

         MemtableAllocator allocator = partitionMap.allocator();
         if(previous == null) {
            DecoratedKey cloneKey = allocator.clone(key);
            AtomicBTreePartition empty = new AtomicBTreePartition(this.cfs.metadata, cloneKey);
            int overhead = (int)(cloneKey.getToken().getHeapSize() + (long)ROW_OVERHEAD_HEAP_SIZE);
            allocator.onHeap().allocated((long)overhead);
            partitionMap.updateLiveDataSize(8L);
            partitionMap.put(cloneKey, empty);
            previous = empty;
         }

         return previous.addAllWithSizeDelta(update, indexer, allocator).map((p) -> {
            partitionMap.update(p.update, p.dataSize);
            return Long.valueOf(p.colUpdateTimeDelta);
         });
      };
      return partitionMap.allocator().whenBelowLimits(write, opGroup, TPC.getForCore(coreId), TPCTaskType.WRITE_MEMTABLE);
   }

   public long getLiveDataSize() {
      long total = 0L;
      MemtableSubrange[] var3 = this.subranges;
      int var4 = var3.length;

      for(int var5 = 0; var5 < var4; ++var5) {
         MemtableSubrange subrange = var3[var5];
         total += subrange.liveDataSize();
      }

      return total;
   }

   public long getOperations() {
      long total = 0L;
      MemtableSubrange[] var3 = this.subranges;
      int var4 = var3.length;

      for(int var5 = 0; var5 < var4; ++var5) {
         MemtableSubrange subrange = var3[var5];
         total += subrange.currentOperations();
      }

      return total;
   }

   public int partitionCount() {
      int total = 0;
      MemtableSubrange[] var2 = this.subranges;
      int var3 = var2.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         MemtableSubrange subrange = var2[var4];
         total += subrange.size();
      }

      return total;
   }

   public long getMinTimestamp() {
      long min = 9223372036854775807L;
      MemtableSubrange[] var3 = this.subranges;
      int var4 = var3.length;

      for(int var5 = 0; var5 < var4; ++var5) {
         MemtableSubrange subrange = var3[var5];
         min = Long.min(min, subrange.minTimestamp());
      }

      return min;
   }

   public int getMinLocalDeletionTime() {
      int min = 2147483647;
      MemtableSubrange[] var2 = this.subranges;
      int var3 = var2.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         MemtableSubrange subrange = var2[var4];
         min = Integer.min(min, subrange.minLocalDeletionTime());
      }

      return min;
   }

   public String toString() {
      Memtable.MemoryUsage usage = this.getMemoryUsage();
      return String.format("Memtable-%s@%s(%s serialized bytes, %s ops, %.0f%%/%.0f%% of on/off-heap limit)", new Object[]{this.cfs.name, Integer.valueOf(this.hashCode()), FBUtilities.prettyPrintMemory(this.getLiveDataSize()), Long.valueOf(this.getOperations()), Float.valueOf(100.0F * usage.ownershipRatioOnHeap), Float.valueOf(100.0F * usage.ownershipRatioOffHeap)});
   }

   private Flow<FlowableUnfilteredPartition> getCorePartitions(final int coreId, final ColumnFilter columnFilter, final DataRange dataRange, final boolean filterStart, final boolean filterStop) {
      final AbstractBounds<PartitionPosition> keyRange = dataRange.keyRange();
      boolean isBound = keyRange instanceof Bounds;
      final boolean includeStart = isBound || keyRange instanceof IncludingExcludingBounds;
      final boolean includeStop = isBound || keyRange instanceof Range;
      return new FlowSource<FlowableUnfilteredPartition>() {
         private Iterator<AtomicBTreePartition> currentPartitions;

         private Iterator<AtomicBTreePartition> getTrimmedSubRange() {
            MemtableSubrange memtableSubrange = Memtable.this.subranges[coreId];
            return filterStart && filterStop?memtableSubrange.subIterator((PartitionPosition)keyRange.left, includeStart, (PartitionPosition)keyRange.right, includeStop, MemtableSubrange.DataAccess.ON_HEAP):(filterStart?memtableSubrange.tailIterator((PartitionPosition)keyRange.left, includeStart, MemtableSubrange.DataAccess.ON_HEAP):(filterStop?memtableSubrange.headIterator((PartitionPosition)keyRange.right, includeStop, MemtableSubrange.DataAccess.ON_HEAP):memtableSubrange.iterator(MemtableSubrange.DataAccess.ON_HEAP)));
         }

         public void requestFirst(FlowSubscriber<FlowableUnfilteredPartition> subscriber, FlowSubscriptionRecipient subscriptionRecipient) {
            this.subscribe(subscriber, subscriptionRecipient);
            this.currentPartitions = this.getTrimmedSubRange();
            if(!this.currentPartitions.hasNext()) {
               subscriber.onComplete();
            } else {
               this.requestNext();
            }

         }

         public void requestNext() {
            AtomicBTreePartition partition = (AtomicBTreePartition)this.currentPartitions.next();
            DecoratedKey key = partition.partitionKey();
            ClusteringIndexFilter filter = dataRange.clusteringIndexFilter(key);
            FlowableUnfilteredPartition fup = filter.getFlowableUnfilteredPartition(columnFilter.withPartitionColumnsVerified(partition.columns()), partition);
            if(this.currentPartitions.hasNext()) {
               this.subscriber.onNext(fup);
            } else {
               this.subscriber.onFinal(fup);
            }

         }

         public void close() throws Exception {
         }
      };
   }

   private Flow<FlowableUnfilteredPartition> getMergedPartitions(ColumnFilter columnFilter, DataRange dataRange) {
      AbstractBounds<PartitionPosition> keyRange = dataRange.keyRange();
      boolean startIsMin = ((PartitionPosition)keyRange.left).isMinimum();
      boolean stopIsMin = ((PartitionPosition)keyRange.right).isMinimum();
      List<Flow<FlowableUnfilteredPartition>> partitionFlows = new ArrayList(this.subranges.length);

      for(int i = 0; i < this.subranges.length; ++i) {
         partitionFlows.add(this.getCorePartitions(i, columnFilter, dataRange, !startIsMin, !stopIsMin));
      }

      return Flow.merge(partitionFlows, Comparator.comparing(FlowablePartitionBase::partitionKey), new Memtable.MergeReducer());
   }

   private Flow<FlowableUnfilteredPartition> getSequentialPartitions(ColumnFilter columnFilter, DataRange dataRange) {
      AbstractBounds<PartitionPosition> keyRange = dataRange.keyRange();
      boolean startIsMin = ((PartitionPosition)keyRange.left).isMinimum();
      boolean stopIsMin = ((PartitionPosition)keyRange.right).isMinimum();
      int start = startIsMin?0:TPC.getCoreForBound(this.boundaries, (PartitionPosition)keyRange.left);
      int end = stopIsMin?this.subranges.length - 1:TPC.getCoreForBound(this.boundaries, (PartitionPosition)keyRange.right);
      return start == end?this.getCorePartitions(start, columnFilter, dataRange, !startIsMin, !stopIsMin):Flow.concat(() -> {
         return new AbstractIterator<Flow<FlowableUnfilteredPartition>>() {
            int current = start;

            protected Flow<FlowableUnfilteredPartition> computeNext() {
               int i = this.current++;
               return i <= end?Memtable.this.getCorePartitions(i, columnFilter, dataRange, i == start && !startIsMin, i == end && !stopIsMin):(Flow)this.endOfData();
            }
         };
      });
   }

   public Flow<FlowableUnfilteredPartition> makePartitionFlow(ColumnFilter columnFilter, DataRange dataRange) {
      return !this.metadata.partitioner.equals(DatabaseDescriptor.getPartitioner())?this.getMergedPartitions(columnFilter, dataRange):this.getSequentialPartitions(columnFilter, dataRange);
   }

   List<Memtable.FlushRunnable> createFlushRunnables(LifecycleTransaction txn) {
      return this.createFlushRunnables(txn, this.cfs.getDiskBoundaries());
   }

   @VisibleForTesting
   List<Memtable.FlushRunnable> createFlushRunnables(LifecycleTransaction txn, DiskBoundaries diskBoundaries) {
      List<PartitionPosition> boundaries = diskBoundaries.positions;
      List<Directories.DataDirectory> locations = diskBoundaries.directories;
      if(boundaries == null) {
         return Collections.singletonList(new Memtable.FlushRunnable(this.subranges, (Directories.DataDirectory)null, (PartitionPosition)null, (PartitionPosition)null, txn));
      } else {
         List<Memtable.FlushRunnable> runnables = new ArrayList(boundaries.size());
         Object rangeStart = this.cfs.getPartitioner().getMinimumToken().minKeyBound();

         try {
            for(int i = 0; i < boundaries.size(); ++i) {
               PartitionPosition t = (PartitionPosition)boundaries.get(i);
               runnables.add(new Memtable.FlushRunnable(this.subranges, (Directories.DataDirectory)locations.get(i), (PartitionPosition)rangeStart, t, txn));
               rangeStart = t;
            }

            return runnables;
         } catch (Throwable var10) {
            Throwable e = var10;

            Memtable.FlushRunnable runnable;
            for(Iterator var8 = runnables.iterator(); var8.hasNext(); e = runnable.abort(e)) {
               runnable = (Memtable.FlushRunnable)var8.next();
            }

            throw Throwables.propagate(e);
         }
      }
   }

   public Flow<Partition> getPartition(DecoratedKey key) {
      int coreId = this.getCoreFor(key);
      return Flow.just(this.subranges[coreId].get(key, MemtableSubrange.DataAccess.ON_HEAP));
   }

   @VisibleForTesting
   public void makeUnflushable() throws Exception {
      for(int i = 0; i < this.subranges.length; ++i) {
         MemtableSubrange subrange = this.subranges[i];
         Threads.evaluateOnCore(() -> {
            subrange.makeUnflushable();
            return null;
         }, i, TPCTaskType.UNKNOWN).reduceBlocking((Object)null, (a, b) -> {
            return null;
         });
      }

   }

   private MemtableSubrange[] generatePartitionSubranges(int splits) {
      if(splits == 1) {
         return new MemtableSubrange[]{new MemtableSubrange(0, this.metadata)};
      } else {
         MemtableSubrange[] partitionMapContainer = new MemtableSubrange[splits];

         for(int i = 0; i < splits; ++i) {
            partitionMapContainer[i] = new MemtableSubrange(i, this.cfs.metadata());
         }

         return partitionMapContainer;
      }
   }

   private static int estimateRowOverhead(int count) {
      MemtableAllocator allocator = MEMORY_POOL.newAllocator(-1);
      ConcurrentNavigableMap<PartitionPosition, Object> partitions = new ConcurrentSkipListMap();
      Object val = new Object();

      for(int i = 0; i < count; ++i) {
         partitions.put(allocator.clone(new BufferDecoratedKey(new Murmur3Partitioner.LongToken((long)i), ByteBufferUtil.EMPTY_BYTE_BUFFER)), val);
      }

      double avgSize = (double)ObjectSizes.measureDeep(partitions) / (double)count;
      int rowOverhead = (int)(avgSize - Math.floor(avgSize) < 0.05D?Math.floor(avgSize):Math.ceil(avgSize));
      rowOverhead = (int)((long)rowOverhead - ObjectSizes.measureDeep(new Murmur3Partitioner.LongToken(0L)));
      rowOverhead = (int)((long)rowOverhead + AtomicBTreePartition.EMPTY_SIZE);
      allocator.setDiscarding();
      allocator.setDiscarded();
      return rowOverhead;
   }

   class FlushRunnable implements Callable<SSTableMultiWriter> {
      private final long estimatedSize;
      private final List<Iterator<AtomicBTreePartition>> toFlush;
      private final boolean isBatchLogTable;
      private final SSTableMultiWriter writer;
      private final PartitionPosition from;
      private final PartitionPosition to;
      private final int keyCount;
      private final AtomicReference<Memtable.FlushRunnableWriterState> state;

      private FlushRunnable(MemtableSubrange[] subranges, Directories.DataDirectory flushLocation, PartitionPosition from, PartitionPosition to, LifecycleTransaction txn) {
         this.toFlush = new ArrayList(subranges.length);
         this.from = from;
         this.to = to;
         long keySize = 0L;
         int keyCount = 0;
         long liveDataSize = 0L;
         MemtableSubrange.ColumnsCollector columnsCollector = new MemtableSubrange.ColumnsCollector(Memtable.this.metadata.regularAndStaticColumns());
         EncodingStats stats = EncodingStats.NO_STATS;
         MemtableSubrange[] var14 = subranges;
         int var15 = subranges.length;

         for(int var16 = 0; var16 < var15; ++var16) {
            MemtableSubrange partitionSubrange = var14[var16];
            Pair<Iterator<PartitionPosition>, Iterator<AtomicBTreePartition>> p = partitionSubrange.iterators(from, to, MemtableSubrange.DataAccess.UNSAFE);
            Iterator<PartitionPosition> keyIt = (Iterator)p.left;
            Iterator<AtomicBTreePartition> partitionIt = (Iterator)p.right;
            if(keyIt.hasNext()) {
               while(keyIt.hasNext()) {
                  PartitionPosition key = (PartitionPosition)keyIt.next();

                  assert key instanceof DecoratedKey;

                  keySize += (long)((DecoratedKey)key).getKey().remaining();
                  ++keyCount;
               }

               this.toFlush.add(partitionIt);
               liveDataSize += partitionSubrange.liveDataSize();
               columnsCollector.merge(partitionSubrange.columnsCollector());
               stats = stats.mergeWith(partitionSubrange.encodingStats());
            }
         }

         this.keyCount = keyCount;
         this.estimatedSize = (long)((double)((long)(keyCount * 8) + keySize + liveDataSize) * 1.2D);
         this.isBatchLogTable = Memtable.this.cfs.name.equals("batches") && Memtable.this.cfs.keyspace.getName().equals("system");
         if(flushLocation == null) {
            this.writer = this.createFlushWriter(txn, Memtable.this.cfs.newSSTableDescriptor(this.getDirectories().getWriteableLocationAsFile(this.estimatedSize)), columnsCollector.get(), stats);
         } else {
            File flushTableDir = this.getDirectories().getLocationForDisk(flushLocation);
            if(BlacklistedDirectories.isUnwritable(flushTableDir)) {
               throw new FSWriteError(new IOException("SSTable flush dir has been blacklisted"), flushTableDir.getAbsolutePath());
            }

            if(flushLocation.getAvailableSpace() < this.estimatedSize) {
               throw new FSDiskFullWriteError(new IOException("Insufficient disk space to write " + this.estimatedSize + " bytes"));
            }

            this.writer = this.createFlushWriter(txn, Memtable.this.cfs.newSSTableDescriptor(flushTableDir), columnsCollector.get(), stats);
         }

         this.state = new AtomicReference(Memtable.FlushRunnableWriterState.IDLE);
      }

      protected Directories getDirectories() {
         return Memtable.this.cfs.getDirectories();
      }


      private void writeSortedContents() {
         if (!this.state.compareAndSet(FlushRunnableWriterState.IDLE, FlushRunnableWriterState.RUNNING)) {
            Memtable.logger.debug("Failed to write {}, flushed range = ({}, {}], state: {}", new Object[] { Memtable.this.toString(), this.from, this.to, this.state });
            return;
         }
         Memtable.logger.debug("Writing {}, flushed range = ({}, {}], state: {}, partitioner {}", new Object[] { Memtable.this.toString(), this.from, this.to, this.state, Memtable.this.metadata.partitioner });
         try {
            List<Iterator<AtomicBTreePartition>> partitions = this.toFlush;
            if (!Memtable.this.metadata.partitioner.equals(DatabaseDescriptor.getPartitioner()) && partitions.size() > 1) {
               Comparator comparator=Comparator.comparing(Partition::partitionKey);
               partitions = Collections.singletonList((Iterator<AtomicBTreePartition>)MergeIterator.<AtomicBTreePartition,AtomicBTreePartition>get(
                       partitions, comparator, new MergeReducer()));
            }
            for (final Iterator<AtomicBTreePartition> partitionSet : partitions) {
               if (this.state.get() == FlushRunnableWriterState.ABORTING) {
                  break;
               }
               while (partitionSet.hasNext() && this.state.get() != FlushRunnableWriterState.ABORTING) {
                  final AtomicBTreePartition partition = partitionSet.next();
                  if (this.isBatchLogTable && !partition.partitionLevelDeletion().isLive() && partition.hasRows()) {
                     continue;
                  }
                  if (partition.isEmpty()) {
                     continue;
                  }
                  try (final UnfilteredRowIterator iter = partition.unfilteredIterator()) {
                     this.writer.append(iter);
                  }
                  catch (Throwable t) {
                     Memtable.logger.debug("Error when flushing: {}/{}", (Object)t.getClass().getName(), (Object)t.getMessage());
                     Throwables.propagate(t);
                  }
               }
            }
         }
         finally {
            while (!this.state.compareAndSet(FlushRunnableWriterState.RUNNING, FlushRunnableWriterState.IDLE)) {
               if (this.state.compareAndSet(FlushRunnableWriterState.ABORTING, FlushRunnableWriterState.ABORTED)) {
                  Memtable.logger.debug("Flushing of {} aborted", (Object)this.writer.getFilename());
                  org.apache.cassandra.utils.Throwables.maybeFail(this.writer.abort(null));
               }
            }
            final long bytesFlushed = this.writer.getFilePointer();
            Memtable.logger.debug("Completed flushing {} ({}) for commitlog position {}", new Object[] { this.writer.getFilename(), FBUtilities.prettyPrintMemory(bytesFlushed), Memtable.this.commitLogUpperBound });
            Memtable.this.cfs.metric.bytesFlushed.inc(bytesFlushed);
         }
      }

      public Throwable abort(Throwable throwable) {
         do {
            if(this.state.compareAndSet(Memtable.FlushRunnableWriterState.IDLE, Memtable.FlushRunnableWriterState.ABORTED)) {
               Memtable.logger.debug("Flushing of {} aborted", this.writer.getFilename());
               return this.writer.abort(throwable);
            }
         } while(!this.state.compareAndSet(Memtable.FlushRunnableWriterState.RUNNING, Memtable.FlushRunnableWriterState.ABORTING));

         return throwable;
      }

      @VisibleForTesting
      Memtable.FlushRunnableWriterState state() {
         return (Memtable.FlushRunnableWriterState)this.state.get();
      }

      public SSTableMultiWriter createFlushWriter(LifecycleTransaction txn, Descriptor descriptor, RegularAndStaticColumns columns, EncodingStats stats) {
         MetadataCollector sstableMetadataCollector = (new MetadataCollector(Memtable.this.metadata.comparator)).commitLogIntervals(new IntervalSet((Comparable)Memtable.this.commitLogLowerBound.get(), (Comparable)Memtable.this.commitLogUpperBound.get()));
         return Memtable.this.cfs.createSSTableMultiWriter(descriptor, (long)this.keyCount, 0L, ActiveRepairService.NO_PENDING_REPAIR, sstableMetadataCollector, new SerializationHeader(true, Memtable.this.metadata, columns, stats), txn);
      }

      public SSTableMultiWriter call() {
         this.writeSortedContents();
         return this.writer;
      }
   }

   @VisibleForTesting
   static enum FlushRunnableWriterState {
      IDLE,
      RUNNING,
      ABORTING,
      ABORTED;

      private FlushRunnableWriterState() {
      }
   }

   private static class MergeReducer<P> extends Reducer<P, P> {
      P reduced;

      private MergeReducer() {
      }

      public boolean trivialReduceIsTrivial() {
         return true;
      }

      public void onKeyChange() {
         this.reduced = null;
      }

      public void reduce(int idx, P current) {
         assert this.reduced == null : "partitions are unique so this should have been called only once";

         this.reduced = current;
      }

      public P getReduced() {
         return this.reduced;
      }
   }

   public static class MemoryUsage {
      public float ownershipRatioOnHeap = 0.0F;
      public float ownershipRatioOffHeap = 0.0F;
      public long ownsOnHeap = 0L;
      public long ownsOffHeap = 0L;

      public MemoryUsage() {
      }
   }

   public static final class LastCommitLogPosition extends CommitLogPosition {
      public LastCommitLogPosition(CommitLogPosition copy) {
         super(copy.segmentId, copy.position);
      }
   }
}
