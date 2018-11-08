package org.apache.cassandra.db.rows;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;

public abstract class AbstractUnfilteredRowIterator extends AbstractIterator<Unfiltered> implements UnfilteredRowIterator {
   protected final TableMetadata metadata;
   protected final DecoratedKey partitionKey;
   protected final DeletionTime partitionLevelDeletion;
   protected final RegularAndStaticColumns columns;
   protected final Row staticRow;
   protected final boolean isReverseOrder;
   protected final EncodingStats stats;

   protected AbstractUnfilteredRowIterator(PartitionTrait partition) {
      this(partition.metadata(), partition.partitionKey(), partition.partitionLevelDeletion(), partition.columns(), partition.staticRow(), partition.isReverseOrder(), partition.stats());
   }

   protected AbstractUnfilteredRowIterator(TableMetadata metadata, DecoratedKey partitionKey, DeletionTime partitionLevelDeletion, RegularAndStaticColumns columns, Row staticRow, boolean isReverseOrder, EncodingStats stats) {
      this.metadata = metadata;
      this.partitionKey = partitionKey;
      this.partitionLevelDeletion = partitionLevelDeletion;
      this.columns = columns;
      this.staticRow = staticRow;
      this.isReverseOrder = isReverseOrder;
      this.stats = stats;
   }

   public TableMetadata metadata() {
      return this.metadata;
   }

   public RegularAndStaticColumns columns() {
      return this.columns;
   }

   public boolean isReverseOrder() {
      return this.isReverseOrder;
   }

   public DecoratedKey partitionKey() {
      return this.partitionKey;
   }

   public DeletionTime partitionLevelDeletion() {
      return this.partitionLevelDeletion;
   }

   public Row staticRow() {
      return this.staticRow;
   }

   public EncodingStats stats() {
      return this.stats;
   }

   public void close() {
   }
}
