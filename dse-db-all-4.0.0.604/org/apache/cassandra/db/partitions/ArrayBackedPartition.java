package org.apache.cassandra.db.partitions;

import io.reactivex.functions.Function;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.ArrayBackedRow;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.FlowablePartitions;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.PartitionHeader;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowAndDeletionMergeIterator;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIndexedListIterator;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.flow.Flow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArrayBackedPartition implements Partition {
   private static final Logger logger = LoggerFactory.getLogger(ArrayBackedPartition.class);
   protected final DecoratedKey partitionKey;
   protected final ArrayBackedPartition.Holder holder;
   protected final TableMetadata metadata;

   public static ArrayBackedPartition create(UnfilteredRowIterator iterator) {
      return create((UnfilteredRowIterator)iterator, 16);
   }

   public static ArrayBackedPartition create(UnfilteredRowIterator iterator, int initialRowCapacity) {
      assert initialRowCapacity > 0;

      Row[] rows = new Row[initialRowCapacity];
      int length = 0;
      MutableDeletionInfo.Builder deletionBuilder = MutableDeletionInfo.builder(iterator.partitionLevelDeletion(), iterator.metadata().comparator, iterator.isReverseOrder());

      while(iterator.hasNext()) {
         Unfiltered unfiltered = (Unfiltered)iterator.next();
         if(unfiltered.kind() == Unfiltered.Kind.ROW) {
            if(length == rows.length) {
               Row[] newRows = new Row[Math.max(8, length * 2)];
               System.arraycopy(rows, 0, newRows, 0, length);
               rows = newRows;
            }

            rows[length++] = (Row)unfiltered;
         } else {
            deletionBuilder.add((RangeTombstoneMarker)unfiltered);
         }
      }

      if(iterator.isReverseOrder() && length > 0) {
         for(int i = 0; i < length; ++i) {
            Row tmp = rows[i];
            int swapidx = length - 1 - i;
            rows[i] = rows[swapidx];
            rows[swapidx] = tmp;
         }
      }

      ArrayBackedPartition.Holder holder = new ArrayBackedPartition.Holder(iterator.metadata().regularAndStaticColumns(), rows, length, deletionBuilder.build(), iterator.staticRow(), iterator.stats());
      return new ArrayBackedPartition(iterator.partitionKey(), holder, iterator.metadata());
   }

   public static Flow<Partition> create(FlowableUnfilteredPartition partition) {
      return create((FlowableUnfilteredPartition)partition, 16);
   }

   public static Flow<Partition> create(Flow<FlowableUnfilteredPartition> partitions) {
      return create((Flow)partitions, 16);
   }

   public static Flow<Partition> create(Flow<FlowableUnfilteredPartition> partitions, int initialRowCapacity) {
      return partitions.flatMap((partition) -> {
         return create(partition, initialRowCapacity);
      });
   }

   public static Flow<Partition> create(FlowableUnfilteredPartition partition, int initialRowCapacity) {
      PartitionHeader header = partition.header();
      ClusteringComparator comparator = header.metadata.comparator;
      Row staticRow = partition.staticRow();

      assert initialRowCapacity > 0;

      ArrayBackedPartition.Holder h = new ArrayBackedPartition.Holder(header.columns, initialRowCapacity, staticRow, header.stats);
      MutableDeletionInfo.Builder deletionBuilder = MutableDeletionInfo.builder(header.partitionLevelDeletion, comparator, header.isReverseOrder);
      return partition.content().process((unfiltered) -> {
         if(unfiltered.kind() == Unfiltered.Kind.ROW) {
            h.maybeGrow();
            h.rows[h.length++] = (Row)unfiltered;
         } else {
            deletionBuilder.add((RangeTombstoneMarker)unfiltered);
         }

      }).skippingMap((VOID) -> {
         DeletionInfo deletion = deletionBuilder.build();
         if(h.length == 0 && deletion.isLive() && staticRow.isEmpty()) {
            return null;
         } else {
            h.deletionInfo = deletion;
            if(partition.isReverseOrder() && h.length > 0) {
               for(int i = 0; i < h.length / 2; ++i) {
                  Row tmp = h.rows[i];
                  int swapidx = h.length - 1 - i;
                  h.rows[i] = h.rows[swapidx];
                  h.rows[swapidx] = tmp;
               }
            }

            return new ArrayBackedPartition(header.partitionKey, h, partition.metadata());
         }
      });
   }

   protected ArrayBackedPartition(DecoratedKey partitionKey, ArrayBackedPartition.Holder holder, TableMetadata metadata) {
      this.partitionKey = partitionKey;
      this.holder = holder;
      this.metadata = metadata;
   }

   protected ArrayBackedPartition.Holder holder() {
      return this.holder;
   }

   public TableMetadata metadata() {
      return this.metadata;
   }

   public DecoratedKey partitionKey() {
      return this.partitionKey;
   }

   public DeletionTime partitionLevelDeletion() {
      return this.holder.deletionInfo.getPartitionDeletion();
   }

   public RegularAndStaticColumns columns() {
      return this.holder().columns;
   }

   public EncodingStats stats() {
      return this.holder().stats;
   }

   public int rowCount() {
      return this.holder().length;
   }

   public Row lastRow() {
      ArrayBackedPartition.Holder holder = this.holder();
      return holder.length > 0?holder.rows[holder.length - 1]:null;
   }

   public DeletionInfo deletionInfo() {
      return this.holder().deletionInfo;
   }

   public Row staticRow() {
      return this.holder().staticRow;
   }

   public boolean isEmpty() {
      ArrayBackedPartition.Holder holder = this.holder();
      return holder.deletionInfo.isLive() && holder.length == 0 && holder.staticRow.isEmpty();
   }

   public boolean hasRows() {
      return this.holder().length > 0;
   }

   public Iterator<Row> iterator() {
      return new AbstractIndexedListIterator<Row>(this.holder.length, 0) {
         protected Row get(int index) {
            return ArrayBackedPartition.this.holder.rows[index];
         }
      };
   }

   public Row getRow(Clustering clustering) {
      Row row = (Row)this.searchIterator(ColumnFilter.selection(this.columns()), false).next(clustering);
      return row != null && (clustering != Clustering.STATIC_CLUSTERING || !row.isEmpty())?row:null;
   }

   protected void append(Row row) {
      this.holder.maybeGrow();
      this.holder.rows[this.holder.length++] = row;
   }

   public SearchIterator<Clustering, Row> searchIterator(final ColumnFilter columns, final boolean reversed) {
      final ArrayBackedPartition.Holder holder = this.holder();
      return new SearchIterator<Clustering, Row>() {
         private final SearchIterator<ClusteringPrefix, Row> rawIter = ArrayBackedPartition.this.new ArrayBackedSearchIterator(reversed);
         private final DeletionTime partitionDeletion;

         {
            this.partitionDeletion = holder.deletionInfo.getPartitionDeletion();
         }

         public Row next(Clustering clustering) {
            if(clustering == Clustering.STATIC_CLUSTERING) {
               return ArrayBackedPartition.this.staticRow(columns, true);
            } else {
               Row row = (Row)this.rawIter.next(clustering);
               RangeTombstone rt = holder.deletionInfo.rangeCovering(clustering);
               DeletionTime activeDeletion = this.partitionDeletion;
               if(rt != null && rt.deletionTime().supersedes(activeDeletion)) {
                  activeDeletion = rt.deletionTime();
               }

               return (Row)(row == null?(activeDeletion.isLive()?null:ArrayBackedRow.emptyDeletedRow(clustering, Row.Deletion.regular(activeDeletion))):row.filter(columns, activeDeletion, true, ArrayBackedPartition.this.metadata()));
            }
         }
      };
   }

   public int indexOf(ClusteringPrefix clustering) {
      return Arrays.binarySearch(this.holder.rows, 0, this.holder.length, clustering, this.metadata.comparator);
   }

   public UnfilteredRowIterator unfilteredIterator() {
      return this.unfilteredIterator(ColumnFilter.selection(this.columns()), Slices.ALL, false);
   }

   public UnfilteredRowIterator unfilteredIterator(ColumnFilter selection, Slices slices, boolean reversed) {
      Row staticRow = this.staticRow(selection, false);
      if(slices.size() == 0) {
         DeletionTime partitionDeletion = this.holder.deletionInfo.getPartitionDeletion();
         return UnfilteredRowIterators.noRowsIterator(this.metadata(), this.partitionKey(), staticRow, partitionDeletion, reversed);
      } else {
         return (UnfilteredRowIterator)(slices.size() == 1?this.sliceIterator(selection, slices.get(0), reversed, staticRow):new ArrayBackedPartition.SlicesIterator(selection, slices, reversed, staticRow, null));
      }
   }

   public FlowableUnfilteredPartition unfilteredPartition() {
      return FlowablePartitions.fromIterator(this.unfilteredIterator());
   }

   public FlowableUnfilteredPartition unfilteredPartition(ColumnFilter selection, Slices slices, boolean reversed) {
      return FlowablePartitions.fromIterator(this.unfilteredIterator(selection, slices, reversed));
   }

   private UnfilteredRowIterator sliceIterator(ColumnFilter selection, Slice slice, boolean reversed, Row staticRow) {
      ClusteringBound start = slice.start() == ClusteringBound.BOTTOM?null:slice.start();
      ClusteringBound end = slice.end() == ClusteringBound.TOP?null:slice.end();
      Iterator<Row> rowIter = this.slice(start, end, reversed);
      Iterator<RangeTombstone> deleteIter = this.holder.deletionInfo.rangeIterator(slice, reversed);
      return this.merge(rowIter, deleteIter, selection, reversed, staticRow);
   }

   private Iterator<Row> slice(ClusteringBound start, ClusteringBound end, final boolean reversed) {
      if(this.holder.length == 0) {
         return Collections.emptyIterator();
      } else {
         final int startAt = start == null?0:this.indexOf(start);
         if(startAt < 0) {
            startAt = -startAt - 1;
         }

         final int endAt = end == null?this.holder.length - 1:this.indexOf(end);
         if(endAt < 0) {
            endAt = -endAt - 2;
         }

         if(endAt >= 0 && endAt >= startAt) {
            assert endAt >= startAt;

            return new AbstractIndexedListIterator<Row>(endAt + 1, startAt) {
               protected Row get(int index) {
                  int idx = reversed?endAt - (index - startAt):index;
                  return ArrayBackedPartition.this.holder.rows[idx];
               }
            };
         } else {
            return Collections.emptyIterator();
         }
      }
   }

   private Row staticRow(ColumnFilter columns, boolean setActiveDeletionToRow) {
      DeletionTime partitionDeletion = this.holder().deletionInfo.getPartitionDeletion();
      if(!columns.fetchedColumns().statics.isEmpty() && (!this.holder().staticRow.isEmpty() || !partitionDeletion.isLive())) {
         Row row = this.holder().staticRow.filter(columns, partitionDeletion, setActiveDeletionToRow, this.metadata());
         return row == null?Rows.EMPTY_STATIC_ROW:row;
      } else {
         return Rows.EMPTY_STATIC_ROW;
      }
   }

   private RowAndDeletionMergeIterator merge(Iterator<Row> rowIter, Iterator<RangeTombstone> deleteIter, ColumnFilter selection, boolean reversed, Row staticRow) {
      return new RowAndDeletionMergeIterator(this.metadata(), this.partitionKey(), this.holder().deletionInfo.getPartitionDeletion(), selection, staticRow, reversed, this.holder().stats, rowIter, deleteIter, this.canHaveShadowedData());
   }

   protected boolean canHaveShadowedData() {
      return false;
   }

   public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(String.format("[%s] key=%s partition_deletion=%s columns=%s", new Object[]{this.metadata(), this.metadata().partitionKeyType.getString(this.partitionKey().getKey()), this.partitionLevelDeletion(), this.columns()}));
      if(this.staticRow() != Rows.EMPTY_STATIC_ROW) {
         sb.append("\n    ").append(this.staticRow().toString(this.metadata(), true));
      }

      UnfilteredRowIterator iter = this.unfilteredIterator();
      Throwable var3 = null;

      try {
         while(iter.hasNext()) {
            sb.append("\n    ").append(((Unfiltered)iter.next()).toString(this.metadata(), true));
         }
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

      return sb.toString();
   }

   class ArrayBackedSearchIterator implements SearchIterator<ClusteringPrefix, Row> {
      final boolean reversed;
      int index;

      ArrayBackedSearchIterator(boolean reversed) {
         this.reversed = reversed;
         this.index = reversed?ArrayBackedPartition.this.holder.length - 1:0;
      }

      public Row next(ClusteringPrefix key) {
         int start = this.reversed?0:this.index;
         int end = this.reversed?this.index:ArrayBackedPartition.this.holder.length;
         this.index = Arrays.binarySearch(ArrayBackedPartition.this.holder.rows, start, end, key, ArrayBackedPartition.this.metadata.comparator);
         if(this.index < 0) {
            this.index = -this.index - 1;
            return null;
         } else {
            return ArrayBackedPartition.this.holder.rows[this.index++];
         }
      }
   }

   class ArrayBackedUnfilteredRowIterator extends AbstractIterator<Unfiltered> implements UnfilteredRowIterator {
      final ArrayBackedPartition.Holder holder = ArrayBackedPartition.this.holder();
      final boolean reversed;
      final Row staticRow;
      final RegularAndStaticColumns columns;
      int index;

      ArrayBackedUnfilteredRowIterator(boolean reversed, Row staticRow, RegularAndStaticColumns columns) {
         this.reversed = reversed;
         this.index = reversed?this.holder.length - 1:0;
         this.staticRow = staticRow;
         this.columns = columns;
      }

      public TableMetadata metadata() {
         return ArrayBackedPartition.this.metadata;
      }

      public boolean isReverseOrder() {
         return this.reversed;
      }

      public RegularAndStaticColumns columns() {
         return this.columns;
      }

      public DecoratedKey partitionKey() {
         return ArrayBackedPartition.this.partitionKey;
      }

      public Row staticRow() {
         return this.staticRow;
      }

      public DeletionTime partitionLevelDeletion() {
         return this.holder.deletionInfo.getPartitionDeletion();
      }

      public EncodingStats stats() {
         return this.holder.stats;
      }

      protected Unfiltered computeNext() {
         if(this.index >= 0 && this.index < this.holder.length) {
            Unfiltered next = this.holder.rows[this.index];
            this.index += this.reversed?-1:1;
            return next;
         } else {
            return (Unfiltered)this.endOfData();
         }
      }
   }

   class SlicesIterator extends ArrayBackedPartition.ArrayBackedUnfilteredRowIterator {
      private final Slices slices;
      private int idx;
      private Iterator<Unfiltered> currentSlice;
      private final ColumnFilter selection;

      private SlicesIterator(ColumnFilter selection, Slices slices, boolean isReversed, Row staticRow) {
         super(isReversed, staticRow, selection.fetchedColumns());
         this.selection = selection;
         this.slices = slices;
      }

      protected Unfiltered computeNext() {
         while(true) {
            if(this.currentSlice == null) {
               if(this.idx >= this.slices.size()) {
                  return (Unfiltered)this.endOfData();
               }

               int sliceIdx = this.reversed?this.slices.size() - this.idx - 1:this.idx;
               this.currentSlice = ArrayBackedPartition.this.sliceIterator(this.selection, this.slices.get(sliceIdx), this.reversed, Rows.EMPTY_STATIC_ROW);
               ++this.idx;
            }

            if(this.currentSlice.hasNext()) {
               return (Unfiltered)this.currentSlice.next();
            }

            this.currentSlice = null;
         }
      }
   }

   public static final class Holder {
      RegularAndStaticColumns columns;
      DeletionInfo deletionInfo;
      Row[] rows;
      int length;
      Row staticRow;
      EncodingStats stats;

      Holder(RegularAndStaticColumns columns, Row[] data, int length, DeletionInfo deletionInfo, Row staticRow, EncodingStats stats) {
         this.columns = columns;
         this.rows = data;
         this.length = length;
         this.deletionInfo = deletionInfo;
         this.staticRow = staticRow == null?Rows.EMPTY_STATIC_ROW:staticRow;
         this.stats = stats;
      }

      Holder(RegularAndStaticColumns columns, int initialRows, Row staticRow, EncodingStats stats) {
         this.columns = columns;
         this.rows = new Row[Math.max(initialRows, 2)];
         this.length = 0;
         this.deletionInfo = DeletionInfo.LIVE;
         this.staticRow = staticRow == null?Rows.EMPTY_STATIC_ROW:staticRow;
         this.stats = stats;
      }

      void maybeGrow() {
         if(this.length == this.rows.length) {
            Row[] newRows = new Row[Math.max(8, this.length * 2)];
            System.arraycopy(this.rows, 0, newRows, 0, this.length);
            this.rows = newRows;
         }

      }
   }
}
