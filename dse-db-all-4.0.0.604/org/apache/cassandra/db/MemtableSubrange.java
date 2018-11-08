package org.apache.cassandra.db;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Consumer;
import org.apache.cassandra.db.partitions.AtomicBTreePartition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.memory.EnsureOnHeap;
import org.apache.cassandra.utils.memory.MemtableAllocator;

class MemtableSubrange {
   private volatile long minTimestamp;
   private volatile int minLocalDeletionTime;
   private volatile long liveDataSize;
   private volatile long currentOperations;
   private final NavigableMap<PartitionPosition, AtomicBTreePartition> data;
   private final MemtableSubrange.ColumnsCollector columnsCollector;
   private final MemtableSubrange.StatsCollector statsCollector;
   private final MemtableAllocator allocator;

   MemtableSubrange(int coreId, TableMetadata metadata) {
      this(metadata, Memtable.MEMORY_POOL.newAllocator(coreId));
   }

   @VisibleForTesting
   MemtableSubrange(TableMetadata metadata, MemtableAllocator allocator) {
      this.minTimestamp = 9223372036854775807L;
      this.minLocalDeletionTime = 2147483647;
      this.liveDataSize = 0L;
      this.currentOperations = 0L;
      this.data = new ConcurrentSkipListMap();
      this.columnsCollector = new MemtableSubrange.ColumnsCollector(metadata.regularAndStaticColumns());
      this.statsCollector = new MemtableSubrange.StatsCollector();
      this.allocator = allocator;
   }

   private boolean copyRequired(MemtableSubrange.DataAccess dataAccess) {
      return dataAccess == MemtableSubrange.DataAccess.ON_HEAP && !this.allocator().onHeapOnly();
   }

   public AtomicBTreePartition get(PartitionPosition key, MemtableSubrange.DataAccess dataAccess) {
      AtomicBTreePartition ret = (AtomicBTreePartition)this.data.get(key);
      return this.copyRequired(dataAccess) && ret != null?ret.ensureOnHeap(this.allocator):ret;
   }

   private Iterator<AtomicBTreePartition> copyOnHeap(final Iterator<AtomicBTreePartition> it) {
      return new AbstractIterator<AtomicBTreePartition>() {
         protected AtomicBTreePartition computeNext() {
            return it.hasNext()?((AtomicBTreePartition)it.next()).ensureOnHeap(MemtableSubrange.this.allocator):(AtomicBTreePartition)this.endOfData();
         }
      };
   }

   Iterator<AtomicBTreePartition> subIterator(PartitionPosition fromPosition, boolean fromInclusive, PartitionPosition toPosition, boolean toInclusive, MemtableSubrange.DataAccess dataAccess) {
      Iterator<AtomicBTreePartition> it = this.data.subMap(fromPosition, fromInclusive, toPosition, toInclusive).values().iterator();
      return this.copyRequired(dataAccess)?this.copyOnHeap(it):it;
   }

   Iterator<AtomicBTreePartition> headIterator(PartitionPosition toPosition, boolean inclusive, MemtableSubrange.DataAccess dataAccess) {
      Iterator<AtomicBTreePartition> it = this.data.headMap(toPosition, inclusive).values().iterator();
      return this.copyRequired(dataAccess)?this.copyOnHeap(it):it;
   }

   Iterator<AtomicBTreePartition> tailIterator(PartitionPosition fromPosition, boolean inclusive, MemtableSubrange.DataAccess dataAccess) {
      Iterator<AtomicBTreePartition> it = this.data.tailMap(fromPosition, inclusive).values().iterator();
      return this.copyRequired(dataAccess)?this.copyOnHeap(it):it;
   }

   public Iterator<AtomicBTreePartition> iterator(MemtableSubrange.DataAccess dataAccess) {
      Iterator<AtomicBTreePartition> it = this.data.values().iterator();
      return this.copyRequired(dataAccess)?this.copyOnHeap(it):it;
   }

   public Pair<Iterator<PartitionPosition>, Iterator<AtomicBTreePartition>> iterators(PartitionPosition from, PartitionPosition to, MemtableSubrange.DataAccess dataAccess) {
      assert from == null && to == null || from != null && to != null : "from and to must either both be null or both not null";

      final SortedMap<PartitionPosition, AtomicBTreePartition> map = from == null?this.data:this.data.subMap(from, true, to, false);
      return !this.copyRequired(dataAccess)?Pair.create(map.keySet().iterator(), map.values().iterator()):Pair.create(new AbstractIterator<PartitionPosition>() {
         Iterator<PartitionPosition> keys = map.keySet().iterator();
         EnsureOnHeap onHeap = new EnsureOnHeap();

         protected PartitionPosition computeNext() {
            return (PartitionPosition)(this.keys.hasNext()?this.onHeap.applyToPartitionKey((DecoratedKey)this.keys.next()):(PartitionPosition)this.endOfData());
         }
      }, new AbstractIterator<AtomicBTreePartition>() {
         Iterator<AtomicBTreePartition> values = map.values().iterator();

         protected AtomicBTreePartition computeNext() {
            return this.values.hasNext()?((AtomicBTreePartition)this.values.next()).ensureOnHeap(MemtableSubrange.this.allocator):(AtomicBTreePartition)this.endOfData();
         }
      });
   }

   public void put(DecoratedKey key, AtomicBTreePartition partition) {
      this.data.put(key, partition);
   }

   public void update(PartitionUpdate partitionUpdate, long dataSize) {
      this.updateTimestamp(partitionUpdate.stats().minTimestamp);
      this.updateLocalDeletionTime(partitionUpdate.stats().minLocalDeletionTime);
      this.updateLiveDataSize(dataSize);
      this.updateCurrentOperations((long)partitionUpdate.operationCount());
      this.columnsCollector.update(partitionUpdate.columns());
      this.statsCollector.update(partitionUpdate.stats());
   }

   public boolean isEmpty() {
      return this.data.isEmpty();
   }

   public int size() {
      return this.data.size();
   }

   private void updateTimestamp(long timestamp) {
      if(timestamp < this.minTimestamp) {
         this.minTimestamp = timestamp;
      }

   }

   void updateLiveDataSize(long size) {
      this.liveDataSize += size;
   }

   private void updateCurrentOperations(long op) {
      this.currentOperations += op;
   }

   private void updateLocalDeletionTime(int localDeletionTime) {
      if(localDeletionTime < this.minLocalDeletionTime) {
         this.minLocalDeletionTime = localDeletionTime;
      }

   }

   MemtableSubrange.ColumnsCollector columnsCollector() {
      return this.columnsCollector;
   }

   EncodingStats encodingStats() {
      return this.statsCollector.stats;
   }

   long minTimestamp() {
      return this.minTimestamp;
   }

   int minLocalDeletionTime() {
      return this.minLocalDeletionTime;
   }

   long liveDataSize() {
      return this.liveDataSize;
   }

   @VisibleForTesting
   void makeUnflushable() {
      this.liveDataSize += 1125899906842624L;
   }

   long currentOperations() {
      return this.currentOperations;
   }

   MemtableAllocator allocator() {
      return this.allocator;
   }

   static class StatsCollector {
      private EncodingStats stats;

      StatsCollector() {
         this.stats = EncodingStats.NO_STATS;
      }

      public void update(EncodingStats newStats) {
         this.stats = this.stats.mergeWith(newStats);
      }

      public EncodingStats get() {
         return this.stats;
      }
   }

   static class ColumnsCollector {
      private final Set<ColumnMetadata> columns = new HashSet();

      ColumnsCollector(RegularAndStaticColumns columns) {
         Columns var10000 = columns.statics;
         Set var10001 = this.columns;
         this.columns.getClass();
         var10000.apply(var10001::add);
         var10000 = columns.regulars;
         var10001 = this.columns;
         this.columns.getClass();
         var10000.apply(var10001::add);
      }

      public void update(RegularAndStaticColumns columns) {
         Columns var10000 = columns.statics;
         Set var10001 = this.columns;
         this.columns.getClass();
         var10000.apply(var10001::add);
         var10000 = columns.regulars;
         var10001 = this.columns;
         this.columns.getClass();
         var10000.apply(var10001::add);
      }

      public RegularAndStaticColumns get() {
         return RegularAndStaticColumns.builder().addAll((Iterable)this.columns).build();
      }

      public void merge(MemtableSubrange.ColumnsCollector other) {
         this.columns.addAll(other.columns);
      }
   }

   static enum DataAccess {
      UNSAFE,
      ON_HEAP;

      private DataAccess() {
      }
   }
}
