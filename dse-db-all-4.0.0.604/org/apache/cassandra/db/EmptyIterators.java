package org.apache.cassandra.db;

import java.util.NoSuchElementException;
import org.apache.cassandra.db.partitions.BasePartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.TableMetadata;

public class EmptyIterators {
   public EmptyIterators() {
   }

   public static UnfilteredPartitionIterator unfilteredPartition(TableMetadata metadata) {
      return new EmptyIterators.EmptyUnfilteredPartitionIterator(metadata);
   }

   public static PartitionIterator partition() {
      return EmptyIterators.EmptyPartitionIterator.instance;
   }

   public static UnfilteredRowIterator unfilteredRow(TableMetadata metadata, DecoratedKey partitionKey, boolean isReverseOrder, Row staticRow, DeletionTime partitionDeletion) {
      RegularAndStaticColumns columns = RegularAndStaticColumns.NONE;
      if(!staticRow.isEmpty()) {
         columns = new RegularAndStaticColumns(Columns.from(staticRow.columns()), Columns.NONE);
      } else {
         staticRow = Rows.EMPTY_STATIC_ROW;
      }

      if(partitionDeletion.isLive()) {
         partitionDeletion = DeletionTime.LIVE;
      }

      return new EmptyIterators.EmptyUnfilteredRowIterator(columns, metadata, partitionKey, isReverseOrder, staticRow, partitionDeletion);
   }

   public static UnfilteredRowIterator unfilteredRow(TableMetadata metadata, DecoratedKey partitionKey, boolean isReverseOrder) {
      return new EmptyIterators.EmptyUnfilteredRowIterator(RegularAndStaticColumns.NONE, metadata, partitionKey, isReverseOrder, Rows.EMPTY_STATIC_ROW, DeletionTime.LIVE);
   }

   public static RowIterator row(TableMetadata metadata, DecoratedKey partitionKey, boolean isReverseOrder) {
      return new EmptyIterators.EmptyRowIterator(metadata, partitionKey, isReverseOrder, Rows.EMPTY_STATIC_ROW);
   }

   private static class EmptyRowIterator extends EmptyIterators.EmptyBaseRowIterator<Row> implements RowIterator {
      public EmptyRowIterator(TableMetadata metadata, DecoratedKey partitionKey, boolean isReverseOrder, Row staticRow) {
         super(RegularAndStaticColumns.NONE, metadata, partitionKey, isReverseOrder, staticRow);
      }
   }

   public static class EmptyUnfilteredRowIterator extends EmptyIterators.EmptyBaseRowIterator<Unfiltered> implements UnfilteredRowIterator {
      DeletionTime partitionLevelDeletion;

      public EmptyUnfilteredRowIterator(RegularAndStaticColumns columns, TableMetadata metadata, DecoratedKey partitionKey, boolean isReverseOrder, Row staticRow, DeletionTime partitionLevelDeletion) {
         super(columns, metadata, partitionKey, isReverseOrder, staticRow);
         this.partitionLevelDeletion = partitionLevelDeletion;
      }

      public boolean isEmpty() {
         return this.partitionLevelDeletion == DeletionTime.LIVE && super.isEmpty();
      }

      public DeletionTime partitionLevelDeletion() {
         return this.partitionLevelDeletion;
      }

      public EncodingStats stats() {
         return EncodingStats.NO_STATS;
      }

      public void reuse(DecoratedKey partitionKey, boolean isReverseOrder, DeletionTime partitionLevelDeletion) {
         this.partitionKey = partitionKey;
         this.isReverseOrder = isReverseOrder;
         this.partitionLevelDeletion = partitionLevelDeletion;
      }
   }

   private static class EmptyBaseRowIterator<U extends Unfiltered> implements BaseRowIterator<U> {
      final RegularAndStaticColumns columns;
      final TableMetadata metadata;
      DecoratedKey partitionKey;
      boolean isReverseOrder;
      final Row staticRow;

      EmptyBaseRowIterator(RegularAndStaticColumns columns, TableMetadata metadata, DecoratedKey partitionKey, boolean isReverseOrder, Row staticRow) {
         this.columns = columns;
         this.metadata = metadata;
         this.partitionKey = partitionKey;
         this.isReverseOrder = isReverseOrder;
         this.staticRow = staticRow;
      }

      public TableMetadata metadata() {
         return this.metadata;
      }

      public boolean isReverseOrder() {
         return this.isReverseOrder;
      }

      public RegularAndStaticColumns columns() {
         return this.columns;
      }

      public DecoratedKey partitionKey() {
         return this.partitionKey;
      }

      public Row staticRow() {
         return this.staticRow;
      }

      public void close() {
      }

      public boolean isEmpty() {
         return this.staticRow == Rows.EMPTY_STATIC_ROW;
      }

      public boolean hasNext() {
         return false;
      }

      public U next() {
         throw new NoSuchElementException();
      }
   }

   private static class EmptyPartitionIterator extends EmptyIterators.EmptyBasePartitionIterator<RowIterator> implements PartitionIterator {
      public static final EmptyIterators.EmptyPartitionIterator instance = new EmptyIterators.EmptyPartitionIterator();

      private EmptyPartitionIterator() {
      }
   }

   private static class EmptyUnfilteredPartitionIterator extends EmptyIterators.EmptyBasePartitionIterator<UnfilteredRowIterator> implements UnfilteredPartitionIterator {
      final TableMetadata metadata;

      public EmptyUnfilteredPartitionIterator(TableMetadata metadata) {
         this.metadata = metadata;
      }

      public TableMetadata metadata() {
         return this.metadata;
      }
   }

   private static class EmptyBasePartitionIterator<R extends BaseRowIterator<?>> implements BasePartitionIterator<R> {
      EmptyBasePartitionIterator() {
      }

      public void close() {
      }

      public boolean hasNext() {
         return false;
      }

      public R next() {
         throw new NoSuchElementException();
      }
   }
}
