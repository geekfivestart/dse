package org.apache.cassandra.db.partitions;

import io.reactivex.functions.Function;
import java.util.Iterator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.db.rows.PartitionHeader;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.flow.Flow;

public class FilteredPartition extends ImmutableBTreePartition {
   public FilteredPartition(RowIterator rows) {
      super(rows.metadata(), rows.partitionKey(), build(rows, DeletionInfo.LIVE, false, 16));
   }

   public FilteredPartition(PartitionHeader header, AbstractBTreePartition.Holder holder) {
      super(header.metadata, header.partitionKey, holder);
   }

   public static FilteredPartition empty(SinglePartitionReadCommand command) {
      return create(EmptyIterators.row(command.metadata(), command.partitionKey(), command.clusteringIndexFilter().isReversed()));
   }

   public static FilteredPartition create(RowIterator iterator) {
      return new FilteredPartition(iterator);
   }

   public static Flow<FilteredPartition> create(FlowablePartition partition) {
      return build(partition, DeletionInfo.LIVE, false, 16).map((holder) -> {
         return new FilteredPartition(partition.header(), holder);
      });
   }

   public RowIterator rowIterator() {
      final Iterator<Row> iter = this.iterator();
      return new RowIterator() {
         public TableMetadata metadata() {
            return FilteredPartition.this.metadata();
         }

         public boolean isReverseOrder() {
            return false;
         }

         public RegularAndStaticColumns columns() {
            return FilteredPartition.this.columns();
         }

         public DecoratedKey partitionKey() {
            return FilteredPartition.this.partitionKey();
         }

         public Row staticRow() {
            return FilteredPartition.this.staticRow();
         }

         public void close() {
         }

         public boolean hasNext() {
            return iter.hasNext();
         }

         public Row next() {
            return (Row)iter.next();
         }

         public boolean isEmpty() {
            return this.staticRow().isEmpty() && !FilteredPartition.this.hasRows();
         }
      };
   }
}
