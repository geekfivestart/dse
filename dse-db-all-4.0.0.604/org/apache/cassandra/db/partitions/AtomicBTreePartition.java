package org.apache.cassandra.db.partitions;

import io.reactivex.Single;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.UpdateFunction;
import org.apache.cassandra.utils.memory.EnsureOnHeap;
import org.apache.cassandra.utils.memory.HeapAllocator;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AtomicBTreePartition extends AbstractBTreePartition {
   public static final long EMPTY_SIZE = ObjectSizes.measure(new AtomicBTreePartition((TableMetadataRef)null, DatabaseDescriptor.getPartitioner().decorateKey(ByteBuffer.allocate(1))));
   private static final Logger logger = LoggerFactory.getLogger(AtomicBTreePartition.class);
   private volatile AbstractBTreePartition.Holder ref;
   private final TableMetadataRef metadata;

   public AtomicBTreePartition(TableMetadataRef metadata, DecoratedKey partitionKey) {
      this(metadata, partitionKey, EMPTY);
   }

   private AtomicBTreePartition(TableMetadataRef metadata, DecoratedKey partitionKey, AbstractBTreePartition.Holder ref) {
      super(partitionKey);
      this.metadata = metadata;
      this.ref = ref;
   }

   public TableMetadata metadata() {
      return this.metadata.get();
   }

   protected boolean canHaveShadowedData() {
      return true;
   }

   public Single<AtomicBTreePartition.AddResult> addAllWithSizeDelta(PartitionUpdate update, UpdateTransaction indexer, MemtableAllocator allocator) {
      AtomicBTreePartition.RowUpdater updater = new AtomicBTreePartition.RowUpdater(this, allocator, indexer);

      try {
         indexer.start();
         if(!update.deletionInfo().getPartitionDeletion().isLive()) {
            indexer.onPartitionDeletion(update.deletionInfo().getPartitionDeletion());
         }

         if(update.deletionInfo().hasRanges()) {
            update.deletionInfo().rangeIterator(false).forEachRemaining(indexer::onRangeTombstone);
         }

         DeletionInfo newDeletionInfo = this.ref.deletionInfo;
         if(update.deletionInfo().mayModify(this.ref.deletionInfo)) {
            DeletionInfo inputDeletionInfoCopy = update.deletionInfo().copy(HeapAllocator.instance);
            newDeletionInfo = this.ref.deletionInfo.mutableCopy().add(inputDeletionInfoCopy);
            updater.allocated(newDeletionInfo.unsharedHeapSize() - this.ref.deletionInfo.unsharedHeapSize());
         }

         RegularAndStaticColumns newColumns = update.columns().mergeTo(this.ref.columns);
         Row newStatic = update.staticRow();
         newStatic = newStatic.isEmpty()?this.ref.staticRow:(this.ref.staticRow.isEmpty()?updater.apply(newStatic):updater.apply(this.ref.staticRow, newStatic));
         Object[] tree = BTree.update(this.ref.tree, update.metadata().comparator, update, update.rowCount(), updater);
         EncodingStats newStats = this.ref.stats.mergeWith(update.stats());
         this.ref = new AbstractBTreePartition.Holder(newColumns, tree, newDeletionInfo, newStatic, newStats);
         updater.finish();
      } catch (Throwable var13) {
         JVMStabilityInspector.inspectThrowable(var13);
         logger.error("Error updating Partition {}", update.partitionKey, var13);
      } finally {
         return indexer.commit().toSingleDefault(new AtomicBTreePartition.AddResult(updater.dataSize, updater.colUpdateTimeDelta, update));
      }
   }

   public AtomicBTreePartition ensureOnHeap(MemtableAllocator allocator) {
      return (AtomicBTreePartition)(allocator.onHeapOnly()?this:new AtomicBTreePartition.AtomicBTreePartitionOnHeap(this, new EnsureOnHeap()));
   }

   public AbstractBTreePartition.Holder holder() {
      return this.ref;
   }

   private static final class RowUpdater implements UpdateFunction<Row, Row> {
      final AtomicBTreePartition updating;
      final MemtableAllocator allocator;
      final UpdateTransaction indexer;
      final int nowInSec;
      Row.Builder regularBuilder;
      long dataSize;
      long heapSize;
      long colUpdateTimeDelta;
      List<Row> inserted;

      private RowUpdater(AtomicBTreePartition updating, MemtableAllocator allocator, UpdateTransaction indexer) {
         this.colUpdateTimeDelta = 9223372036854775807L;
         this.updating = updating;
         this.allocator = allocator;
         this.indexer = indexer;
         this.nowInSec = FBUtilities.nowInSeconds();
      }

      private Row.Builder builder(Clustering clustering) {
         boolean isStatic = clustering == Clustering.STATIC_CLUSTERING;
         if(isStatic) {
            return this.allocator.rowBuilder();
         } else {
            if(this.regularBuilder == null) {
               this.regularBuilder = this.allocator.rowBuilder();
            }

            return this.regularBuilder;
         }
      }

      public Row apply(Row insert) {
         Row data = Rows.copy(insert, this.builder(insert.clustering())).build();
         this.indexer.onInserted(insert);
         this.dataSize += (long)data.dataSize();
         this.heapSize += data.unsharedHeapSizeExcludingData();
         return data;
      }

      public Row apply(Row existing, Row update) {
         Row.Builder builder = this.builder(existing.clustering());
         this.colUpdateTimeDelta = Math.min(this.colUpdateTimeDelta, Rows.merge(existing, update, builder, this.nowInSec));
         Row reconciled = builder.build();
         this.indexer.onUpdated(existing, reconciled);
         this.dataSize += (long)(reconciled.dataSize() - existing.dataSize());
         this.heapSize += reconciled.unsharedHeapSizeExcludingData() - existing.unsharedHeapSizeExcludingData();
         if(this.inserted == null) {
            this.inserted = new ArrayList();
         }

         this.inserted.add(reconciled);
         return reconciled;
      }

      protected void reset() {
         this.dataSize = 0L;
         this.heapSize = 0L;
         if(this.inserted != null) {
            this.inserted.clear();
         }

      }

      public boolean abortEarly() {
         return false;
      }

      public void allocated(long heapSize) {
         this.heapSize += heapSize;
      }

      protected void finish() {
         this.allocator.onHeap().adjust(this.heapSize);
      }
   }

   private static final class AtomicBTreePartitionOnHeap extends AtomicBTreePartition {
      private final EnsureOnHeap ensureOnHeap;

      private AtomicBTreePartitionOnHeap(AtomicBTreePartition inner, EnsureOnHeap ensureOnHeap) {
         super(inner.metadata, ensureOnHeap.applyToPartitionKey(inner.partitionKey()), inner.ref);
         this.ensureOnHeap = ensureOnHeap;
      }

      public DeletionInfo deletionInfo() {
         return this.ensureOnHeap.applyToDeletionInfo(super.deletionInfo());
      }

      public Row staticRow() {
         return this.ensureOnHeap.applyToStatic(super.staticRow());
      }

      public Row getRow(Clustering clustering) {
         return this.ensureOnHeap.applyToRow(super.getRow(clustering));
      }

      public Row lastRow() {
         return this.ensureOnHeap.applyToRow(super.lastRow());
      }

      public SearchIterator<Clustering, Row> searchIterator(ColumnFilter columns, boolean reversed) {
         return this.ensureOnHeap.applyToPartition(super.searchIterator(columns, reversed));
      }

      public UnfilteredRowIterator unfilteredIterator(ColumnFilter selection, Slices slices, boolean reversed) {
         return this.ensureOnHeap.applyToPartition(super.unfilteredIterator(selection, slices, reversed));
      }

      public UnfilteredRowIterator unfilteredIterator() {
         return this.ensureOnHeap.applyToPartition(super.unfilteredIterator());
      }

      public UnfilteredRowIterator unfilteredIterator(AbstractBTreePartition.Holder current, ColumnFilter selection, Slices slices, boolean reversed) {
         return this.ensureOnHeap.applyToPartition(super.unfilteredIterator(current, selection, slices, reversed));
      }

      public Iterator<Row> iterator() {
         return this.ensureOnHeap.applyToPartition(super.iterator());
      }
   }

   public static class AddResult {
      public final long dataSize;
      public final long colUpdateTimeDelta;
      public final PartitionUpdate update;

      AddResult(long dataSize, long colUpdateTimeDelta, PartitionUpdate update) {
         this.dataSize = dataSize;
         this.colUpdateTimeDelta = colUpdateTimeDelta;
         this.update = update;
      }
   }
}
