package org.apache.cassandra.db.partitions;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.TableMetadata;

public class ImmutableBTreePartition extends AbstractBTreePartition {
   protected final AbstractBTreePartition.Holder holder;
   protected final TableMetadata metadata;

   public ImmutableBTreePartition(TableMetadata metadata, DecoratedKey partitionKey, RegularAndStaticColumns columns, Row staticRow, Object[] tree, DeletionInfo deletionInfo, EncodingStats stats) {
      super(partitionKey);
      this.metadata = metadata;
      this.holder = new AbstractBTreePartition.Holder(columns, tree, deletionInfo, staticRow, stats);
   }

   protected ImmutableBTreePartition(TableMetadata metadata, DecoratedKey partitionKey, AbstractBTreePartition.Holder holder) {
      super(partitionKey);
      this.metadata = metadata;
      this.holder = holder;
   }

   public static Partition create(UnfilteredRowIterator iterator, boolean ordered) {
      return new ImmutableBTreePartition(iterator.metadata(), iterator.partitionKey(), build(iterator, 16, ordered));
   }

   public static Partition create(UnfilteredRowIterator iterator, int initialRowCapacity, boolean ordered) {
      return new ImmutableBTreePartition(iterator.metadata(), iterator.partitionKey(), build(iterator, initialRowCapacity, ordered));
   }

   public TableMetadata metadata() {
      return this.metadata;
   }

   protected AbstractBTreePartition.Holder holder() {
      return this.holder;
   }

   protected boolean canHaveShadowedData() {
      return false;
   }
}
