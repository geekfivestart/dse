package org.apache.cassandra.io.sstable.format.big;

import java.io.IOException;
import java.util.Iterator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.io.sstable.format.AbstractReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.btree.BTree;

class ReverseReader extends AbstractReader {
   protected ReverseReader.ReusablePartitionData buffer;
   protected Iterator<Unfiltered> iterator;
   protected boolean skipFirstIteratedItem;
   protected boolean skipLastIteratedItem;
   final BigTableReader sstable;
   Slice slice;
   ReverseReader.PrepStage prepStage;

   ReverseReader(BigTableReader sstable, Slices slices, FileDataInput file, boolean shouldCloseFile, SerializationHelper helper) {
      super(sstable, slices, file, shouldCloseFile, helper, true);
      this.slice = Slice.ALL;
      this.prepStage = ReverseReader.PrepStage.OPEN;
      this.sstable = sstable;
   }

   protected ReverseReader.ReusablePartitionData createBuffer(int blocksCount) {
      int estimatedRowCount = 16;
      int columnCount = this.metadata.regularColumns().size();
      if(columnCount != 0 && !this.metadata.clusteringColumns().isEmpty()) {
         try {
            int estimatedRowsPerPartition = (int)(this.sstable.getEstimatedColumnCount().percentile(0.75D) / (long)columnCount);
            estimatedRowCount = Math.max(estimatedRowsPerPartition / blocksCount, 1);
         } catch (IllegalStateException var5) {
            ;
         }
      } else {
         estimatedRowCount = 1;
      }

      return new ReverseReader.ReusablePartitionData(this.metadata, estimatedRowCount, null);
   }

   public boolean setForSlice(Slice slice) throws IOException {
      this.slice = slice;
      return super.setForSlice(slice);
   }

   protected void setIterator(Slice slice) {
      assert this.buffer != null;

      this.iterator = this.buffer.built.unfilteredIterator(ColumnFilter.all(this.metadata), Slices.with(this.metadata.comparator, slice), true);
      if(this.iterator.hasNext()) {
         if(this.skipFirstIteratedItem) {
            this.iterator.next();
         }

         if(this.skipLastIteratedItem) {
            this.iterator = new ReverseReader.SkipLastIterator(this.iterator, null);
         }

      }
   }

   protected Unfiltered nextInSlice() throws IOException {
      return this.iterator.hasNext()?(Unfiltered)this.iterator.next():null;
   }

   protected boolean preSliceStep() throws IOException {
      return true;
   }

   protected boolean slicePrepStep() throws IOException {
      if(this.buffer == null) {
         this.buffer = this.createBuffer(1);
      }

      return this.prepStep(false, false);
   }

   protected boolean prepStep(boolean hasNextBlock, boolean hasPreviousBlock) throws IOException {
      switch(null.$SwitchMap$org$apache$cassandra$io$sstable$format$big$ReverseReader$PrepStage[this.prepStage.ordinal()]) {
      case 1:
         this.skipLastIteratedItem = false;
         this.skipFirstIteratedItem = false;
         this.buffer.reset();
         if(this.openMarker != null) {
            this.buffer.add(new RangeTombstoneBoundMarker(this.start, this.openMarker));
            this.skipLastIteratedItem = hasNextBlock;
         }

         this.prepStage = ReverseReader.PrepStage.PROCESSING;
      case 2:
         if(this.deserializer.hasNext() && this.deserializer.compareNextTo(this.end) < 0 && !this.stopReadingDisk()) {
            Unfiltered unfiltered = this.deserializer.readNext();
            if(!unfiltered.isEmpty()) {
               this.buffer.add(unfiltered);
            }

            if(unfiltered.isRangeTombstoneMarker()) {
               this.updateOpenMarker((RangeTombstoneMarker)unfiltered);
            }

            return false;
         } else {
            this.prepStage = ReverseReader.PrepStage.CLOSE;
         }
      case 3:
         if(this.openMarker != null) {
            this.buffer.add(new RangeTombstoneBoundMarker(this.end, this.openMarker));
            this.skipFirstIteratedItem = hasPreviousBlock;
         }

         this.buffer.build();
         this.prepStage = ReverseReader.PrepStage.DONE;
      case 4:
         this.setIterator(this.slice);
      default:
         return true;
      }
   }

   protected RangeTombstoneMarker sliceStartMarker() {
      return null;
   }

   protected RangeTombstoneMarker sliceEndMarker() {
      return null;
   }

   protected boolean stopReadingDisk() throws IOException {
      return false;
   }

   static class SkipLastIterator extends AbstractIterator<Unfiltered> {
      private final Iterator<Unfiltered> iterator;

      private SkipLastIterator(Iterator<Unfiltered> iterator) {
         this.iterator = iterator;
      }

      protected Unfiltered computeNext() {
         if(!this.iterator.hasNext()) {
            return (Unfiltered)this.endOfData();
         } else {
            Unfiltered next = (Unfiltered)this.iterator.next();
            return this.iterator.hasNext()?next:(Unfiltered)this.endOfData();
         }
      }
   }

   static class ReusablePartitionData {
      private final TableMetadata metadata;
      private MutableDeletionInfo.Builder deletionBuilder;
      private MutableDeletionInfo deletionInfo;
      private BTree.Builder<Row> rowBuilder;
      private ImmutableBTreePartition built;

      private ReusablePartitionData(TableMetadata metadata, int initialRowCapacity) {
         this.metadata = metadata;
         this.rowBuilder = BTree.builder(metadata.comparator, initialRowCapacity);
      }

      public void add(Unfiltered unfiltered) {
         if(unfiltered.isRow()) {
            this.rowBuilder.add((Row)unfiltered);
         } else {
            this.deletionBuilder.add((RangeTombstoneMarker)unfiltered);
         }

      }

      public void reset() {
         this.built = null;
         this.rowBuilder = BTree.builder(this.metadata.comparator);
         this.deletionBuilder = MutableDeletionInfo.builder(DeletionTime.LIVE, this.metadata.comparator, false);
      }

      public void build() {
         this.deletionInfo = this.deletionBuilder.build();
         this.built = new ImmutableBTreePartition(this.metadata, (DecoratedKey)null, (RegularAndStaticColumns)null, Rows.EMPTY_STATIC_ROW, this.rowBuilder.build(), this.deletionInfo, EncodingStats.NO_STATS);
         this.deletionBuilder = null;
      }
   }

   static enum PrepStage {
      OPEN,
      PROCESSING,
      CLOSE,
      DONE;

      private PrepStage() {
      }
   }
}
