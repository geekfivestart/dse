package org.apache.cassandra.db.transform;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;

public abstract class Transformation<I extends BaseRowIterator<?>> {
   public Transformation() {
   }

   public void onClose() {
   }

   public void onPartitionClose() {
   }

   protected I applyToPartition(I partition) {
      return partition;
   }

   protected Row applyToRow(Row row) {
      return row;
   }

   protected RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker) {
      return marker;
   }

   protected DecoratedKey applyToPartitionKey(DecoratedKey key) {
      return key;
   }

   public Row applyToStatic(Row row) {
      return row;
   }

   protected DeletionTime applyToDeletion(DeletionTime deletionTime) {
      return deletionTime;
   }

   protected RegularAndStaticColumns applyToPartitionColumns(RegularAndStaticColumns columns) {
      return columns;
   }

   public Unfiltered applyToUnfiltered(Unfiltered item) {
      return (Unfiltered)(item.isRow()?this.applyToRow((Row)item):this.applyToMarker((RangeTombstoneMarker)item));
   }

   public static UnfilteredPartitionIterator apply(UnfilteredPartitionIterator iterator, Transformation<? super UnfilteredRowIterator> transformation) {
      return (UnfilteredPartitionIterator)add(mutable(iterator), (Transformation)transformation);
   }

   public static PartitionIterator apply(PartitionIterator iterator, Transformation<? super RowIterator> transformation) {
      return (PartitionIterator)add(mutable(iterator), (Transformation)transformation);
   }

   public static UnfilteredRowIterator apply(UnfilteredRowIterator iterator, Transformation<?> transformation) {
      return (UnfilteredRowIterator)add(mutable(iterator), (Transformation)transformation);
   }

   public static RowIterator apply(RowIterator iterator, Transformation<?> transformation) {
      return (RowIterator)add(mutable(iterator), (Transformation)transformation);
   }

   static UnfilteredPartitions mutable(UnfilteredPartitionIterator iterator) {
      return iterator instanceof UnfilteredPartitions?(UnfilteredPartitions)iterator:new UnfilteredPartitions(iterator);
   }

   static FilteredPartitions mutable(PartitionIterator iterator) {
      return iterator instanceof FilteredPartitions?(FilteredPartitions)iterator:new FilteredPartitions(iterator);
   }

   static UnfilteredRows mutable(UnfilteredRowIterator iterator) {
      return iterator instanceof UnfilteredRows?(UnfilteredRows)iterator:new UnfilteredRows(iterator);
   }

   static FilteredRows mutable(RowIterator iterator) {
      return iterator instanceof FilteredRows?(FilteredRows)iterator:new FilteredRows(iterator);
   }

   static UnfilteredRows wrapIterator(UnfilteredRowIterator iterator, RegularAndStaticColumns columns) {
      return new UnfilteredRows(iterator, columns);
   }

   static <E extends BaseIterator> E add(E to, Transformation add) {
      to.add(add);
      return to;
   }

   static <E extends BaseIterator> E add(E to, MoreContents add) {
      to.add(add);
      return to;
   }
}
