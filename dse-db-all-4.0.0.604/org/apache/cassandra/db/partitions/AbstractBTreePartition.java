package org.apache.cassandra.db.partitions;

import io.reactivex.functions.Function;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.ArrayBackedRow;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.db.rows.FlowablePartitions;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.PartitionHeader;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowAndDeletionMergeIterator;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.flow.Flow;

public abstract class AbstractBTreePartition implements Partition, Iterable<Row> {
   protected static final AbstractBTreePartition.Holder EMPTY;
   protected final DecoratedKey partitionKey;

   protected abstract AbstractBTreePartition.Holder holder();

   protected abstract boolean canHaveShadowedData();

   protected AbstractBTreePartition(DecoratedKey partitionKey) {
      this.partitionKey = partitionKey;
   }

   public DeletionInfo deletionInfo() {
      return this.holder().deletionInfo;
   }

   public Row staticRow() {
      return this.holder().staticRow;
   }

   public boolean isEmpty() {
      AbstractBTreePartition.Holder holder = this.holder();
      return holder.deletionInfo.isLive() && BTree.isEmpty(holder.tree) && holder.staticRow.isEmpty();
   }

   public boolean hasRows() {
      AbstractBTreePartition.Holder holder = this.holder();
      return !BTree.isEmpty(holder.tree);
   }

   public abstract TableMetadata metadata();

   public DecoratedKey partitionKey() {
      return this.partitionKey;
   }

   public DeletionTime partitionLevelDeletion() {
      return this.deletionInfo().getPartitionDeletion();
   }

   public RegularAndStaticColumns columns() {
      return this.holder().columns;
   }

   public EncodingStats stats() {
      return this.holder().stats;
   }

   public Row getRow(Clustering clustering) {
      Row row = (Row)this.searchIterator(ColumnFilter.selection(this.columns()), false).next(clustering);
      return row != null && (clustering != Clustering.STATIC_CLUSTERING || !row.isEmpty())?row:null;
   }

   private Row staticRow(AbstractBTreePartition.Holder current, ColumnFilter columns, boolean setActiveDeletionToRow) {
      DeletionTime partitionDeletion = current.deletionInfo.getPartitionDeletion();
      if(!columns.fetchedColumns().statics.isEmpty() && (!current.staticRow.isEmpty() || !partitionDeletion.isLive())) {
         Row row = current.staticRow.filter(columns, partitionDeletion, setActiveDeletionToRow, this.metadata());
         return row == null?Rows.EMPTY_STATIC_ROW:row;
      } else {
         return Rows.EMPTY_STATIC_ROW;
      }
   }

   public SearchIterator<Clustering, Row> searchIterator(final ColumnFilter columns, final boolean reversed) {
      final AbstractBTreePartition.Holder current = this.holder();
      return new SearchIterator<Clustering, Row>() {
         private final SearchIterator<Clustering, Row> rawIter;
         private final DeletionTime partitionDeletion;

         {
            this.rawIter = BTree.slice(current.tree, AbstractBTreePartition.this.metadata().comparator, BTree.Dir.desc(reversed));
            this.partitionDeletion = current.deletionInfo.getPartitionDeletion();
         }

         public Row next(Clustering clustering) {
            if(clustering == Clustering.STATIC_CLUSTERING) {
               return AbstractBTreePartition.this.staticRow(current, columns, true);
            } else {
               Row row = (Row)this.rawIter.next(clustering);
               RangeTombstone rt = current.deletionInfo.rangeCovering(clustering);
               DeletionTime activeDeletion = this.partitionDeletion;
               if(rt != null && rt.deletionTime().supersedes(activeDeletion)) {
                  activeDeletion = rt.deletionTime();
               }

               return (Row)(row == null?(activeDeletion.isLive()?null:ArrayBackedRow.emptyDeletedRow(clustering, Row.Deletion.regular(activeDeletion))):row.filter(columns, activeDeletion, true, AbstractBTreePartition.this.metadata()));
            }
         }
      };
   }

   public FlowableUnfilteredPartition unfilteredPartition() {
      return this.unfilteredPartition(ColumnFilter.selection(this.columns()), Slices.ALL, false);
   }

   public FlowableUnfilteredPartition unfilteredPartition(ColumnFilter selection, Slices slices, boolean reversed) {
      return FlowablePartitions.fromIterator(this.unfilteredIterator(this.holder(), selection, slices, reversed));
   }

   public UnfilteredRowIterator unfilteredIterator() {
      return this.unfilteredIterator(ColumnFilter.selection(this.columns()), Slices.ALL, false);
   }

   public UnfilteredRowIterator unfilteredIterator(ColumnFilter selection, Slices slices, boolean reversed) {
      return this.unfilteredIterator(this.holder(), selection, slices, reversed);
   }

   public UnfilteredRowIterator unfilteredIterator(AbstractBTreePartition.Holder current, ColumnFilter selection, Slices slices, boolean reversed) {
      Row staticRow = this.staticRow(current, selection, false);
      if(slices.size() == 0) {
         DeletionTime partitionDeletion = current.deletionInfo.getPartitionDeletion();
         return UnfilteredRowIterators.noRowsIterator(this.metadata(), this.partitionKey(), staticRow, partitionDeletion, reversed);
      } else {
         return (UnfilteredRowIterator)(slices.size() == 1?this.sliceIterator(selection, slices.get(0), reversed, current, staticRow):new AbstractBTreePartition.SlicesIterator(selection, slices, reversed, current, staticRow));
      }
   }

   private UnfilteredRowIterator sliceIterator(ColumnFilter selection, Slice slice, boolean reversed, AbstractBTreePartition.Holder current, Row staticRow) {
      ClusteringBound start = slice.start() == ClusteringBound.BOTTOM?null:slice.start();
      ClusteringBound end = slice.end() == ClusteringBound.TOP?null:slice.end();
      Iterator<Row> rowIter = BTree.slice(current.tree, this.metadata().comparator, start, true, end, true, BTree.Dir.desc(reversed));
      Iterator<RangeTombstone> deleteIter = current.deletionInfo.rangeIterator(slice, reversed);
      return this.merge(rowIter, deleteIter, selection, reversed, current, staticRow);
   }

   private RowAndDeletionMergeIterator merge(Iterator<Row> rowIter, Iterator<RangeTombstone> deleteIter, ColumnFilter selection, boolean reversed, AbstractBTreePartition.Holder current, Row staticRow) {
      return new RowAndDeletionMergeIterator(this.metadata(), this.partitionKey(), current.deletionInfo.getPartitionDeletion(), selection, staticRow, reversed, current.stats, rowIter, deleteIter, this.canHaveShadowedData());
   }

   protected static AbstractBTreePartition.Holder build(UnfilteredRowIterator iterator, int initialRowCapacity) {
      return build(iterator, initialRowCapacity, true);
   }

   public static AbstractBTreePartition.Holder build(FlowableUnfilteredPartition fup, List<Unfiltered> materializedRows) {
      TableMetadata metadata = fup.metadata();
      RegularAndStaticColumns columns = fup.columns();
      boolean reversed = fup.isReverseOrder();
      BTree.Builder<Row> builder = BTree.builder(metadata.comparator, materializedRows.size());
      builder.auto(false);
      MutableDeletionInfo.Builder deletionBuilder = MutableDeletionInfo.builder(fup.partitionLevelDeletion(), metadata.comparator, reversed);
      Iterator var7 = materializedRows.iterator();

      while(var7.hasNext()) {
         Unfiltered unfiltered = (Unfiltered)var7.next();
         if(unfiltered.kind() == Unfiltered.Kind.ROW) {
            builder.add((Row)unfiltered);
         } else {
            deletionBuilder.add((RangeTombstoneMarker)unfiltered);
         }
      }

      if(reversed) {
         builder.reverse();
      }

      return new AbstractBTreePartition.Holder(columns, builder.build(), deletionBuilder.build(), fup.staticRow(), fup.stats());
   }

   protected static AbstractBTreePartition.Holder build(UnfilteredRowIterator iterator, int initialRowCapacity, boolean ordered) {
      TableMetadata metadata = iterator.metadata();
      RegularAndStaticColumns columns = iterator.columns();
      boolean reversed = iterator.isReverseOrder();
      BTree.Builder<Row> builder = BTree.builder(metadata.comparator, initialRowCapacity);
      builder.auto(!ordered);
      MutableDeletionInfo.Builder deletionBuilder = MutableDeletionInfo.builder(iterator.partitionLevelDeletion(), metadata.comparator, reversed);

      while(iterator.hasNext()) {
         Unfiltered unfiltered = (Unfiltered)iterator.next();
         if(unfiltered.kind() == Unfiltered.Kind.ROW) {
            builder.add((Row)unfiltered);
         } else {
            deletionBuilder.add((RangeTombstoneMarker)unfiltered);
         }
      }

      if(reversed) {
         builder.reverse();
      }

      return new AbstractBTreePartition.Holder(columns, builder.build(), deletionBuilder.build(), iterator.staticRow(), iterator.stats());
   }

   protected static Flow<AbstractBTreePartition.Holder> build(FlowableUnfilteredPartition partition, int initialRowCapacity, boolean ordered) {
      PartitionHeader header = partition.header();
      ClusteringComparator comparator = header.metadata.comparator;
      Row staticRow = partition.staticRow();
      MutableDeletionInfo.Builder deletionBuilder = MutableDeletionInfo.builder(header.partitionLevelDeletion, comparator, header.isReverseOrder);
      BTree.Builder builder = BTree.builder(comparator, initialRowCapacity).auto(!ordered);
      return partition.content().process((unfiltered) -> {
         if(unfiltered.kind() == Unfiltered.Kind.ROW) {
            builder.add(unfiltered);
         } else {
            deletionBuilder.add((RangeTombstoneMarker)unfiltered);
         }

      }).skippingMap((VOID) -> {
         DeletionInfo deletion = deletionBuilder.build();
         if(builder.isEmpty() && deletion.isLive() && staticRow.isEmpty()) {
            return null;
         } else {
            if(header.isReverseOrder) {
               builder.reverse();
            }

            return new AbstractBTreePartition.Holder(header.columns, builder.build(), deletion, staticRow, header.stats);
         }
      });
   }

   protected static AbstractBTreePartition.Holder build(RowIterator rows, DeletionInfo deletion, boolean buildEncodingStats, int initialRowCapacity) {
      TableMetadata metadata = rows.metadata();
      RegularAndStaticColumns columns = rows.columns();
      boolean reversed = rows.isReverseOrder();
      BTree.Builder<Row> builder = BTree.builder(metadata.comparator, initialRowCapacity);
      builder.auto(false);

      while(rows.hasNext()) {
         builder.add(rows.next());
      }

      if(reversed) {
         builder.reverse();
      }

      Row staticRow = rows.staticRow();
      Object[] tree = builder.build();
      EncodingStats stats = buildEncodingStats?EncodingStats.Collector.collect(staticRow, BTree.iterator(tree), deletion):EncodingStats.NO_STATS;
      return new AbstractBTreePartition.Holder(columns, tree, deletion, staticRow, stats);
   }

   protected static Flow<AbstractBTreePartition.Holder> build(FlowablePartition partition, DeletionInfo deletion, boolean buildEncodingStats, int initialRowCapacity) {
      TableMetadata metadata = partition.metadata();
      RegularAndStaticColumns columns = partition.columns();
      boolean reversed = partition.isReverseOrder();
      return partition.content().reduce(BTree.builder(metadata.comparator, initialRowCapacity).auto(false), BTree.Builder::add).map((builder) -> {
         if(reversed) {
            builder.reverse();
         }

         Row staticRow = partition.staticRow();
         Object[] tree = builder.build();
         EncodingStats stats = buildEncodingStats?EncodingStats.Collector.collect(staticRow, BTree.iterator(tree), deletion):EncodingStats.NO_STATS;
         return new AbstractBTreePartition.Holder(columns, tree, deletion, staticRow, stats);
      });
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

   public int rowCount() {
      return BTree.size(this.holder().tree);
   }

   public Iterator<Row> iterator() {
      return BTree.iterator(this.holder().tree);
   }

   public Row lastRow() {
      Object[] tree = this.holder().tree;
      return BTree.isEmpty(tree)?null:(Row)BTree.findByIndex(tree, BTree.size(tree) - 1);
   }

   static {
      EMPTY = new AbstractBTreePartition.Holder(RegularAndStaticColumns.NONE, BTree.empty(), DeletionInfo.LIVE, Rows.EMPTY_STATIC_ROW, EncodingStats.NO_STATS);
   }

   public class SlicesIterator extends AbstractBTreePartition.AbstractIterator {
      private final Slices slices;
      private int idx;
      private Iterator<Unfiltered> currentSlice;

      private SlicesIterator(ColumnFilter selection, Slices slices, boolean isReversed, AbstractBTreePartition.Holder current, Row staticRow) {
         super(current, staticRow, selection, isReversed);
         this.slices = slices;
      }

      protected Unfiltered computeNext() {
         while(true) {
            if(this.currentSlice == null) {
               if(this.idx >= this.slices.size()) {
                  return (Unfiltered)this.endOfData();
               }

               int sliceIdx = this.isReverseOrder?this.slices.size() - this.idx - 1:this.idx;
               this.currentSlice = AbstractBTreePartition.this.sliceIterator(this.selection, this.slices.get(sliceIdx), this.isReverseOrder, this.current, Rows.EMPTY_STATIC_ROW);
               ++this.idx;
            }

            if(this.currentSlice.hasNext()) {
               return (Unfiltered)this.currentSlice.next();
            }

            this.currentSlice = null;
         }
      }
   }

   private abstract class AbstractIterator extends AbstractUnfilteredRowIterator {
      final AbstractBTreePartition.Holder current;
      final ColumnFilter selection;

      private AbstractIterator(AbstractBTreePartition.Holder current, Row staticRow, ColumnFilter selection, boolean isReversed) {
         super(AbstractBTreePartition.this.metadata(), AbstractBTreePartition.this.partitionKey(), current.deletionInfo.getPartitionDeletion(), selection.fetchedColumns(), staticRow, isReversed, current.stats);
         this.current = current;
         this.selection = selection;
      }
   }

   public static final class Holder {
      final RegularAndStaticColumns columns;
      final DeletionInfo deletionInfo;
      final Object[] tree;
      final Row staticRow;
      final EncodingStats stats;

      Holder(RegularAndStaticColumns columns, Object[] tree, DeletionInfo deletionInfo, Row staticRow, EncodingStats stats) {
         this.columns = columns;
         this.tree = tree;
         this.deletionInfo = deletionInfo;
         this.staticRow = staticRow == null?Rows.EMPTY_STATIC_ROW:staticRow;
         this.stats = stats;
      }
   }
}
