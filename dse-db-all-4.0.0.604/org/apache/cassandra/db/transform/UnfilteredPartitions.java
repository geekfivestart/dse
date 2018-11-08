package org.apache.cassandra.db.transform;

import org.apache.cassandra.db.partitions.BasePartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.TableMetadata;

final class UnfilteredPartitions extends BasePartitions<UnfilteredRowIterator, UnfilteredPartitionIterator> implements UnfilteredPartitionIterator {
   public UnfilteredPartitions(UnfilteredPartitionIterator input) {
      super((BasePartitionIterator)input);
   }

   public TableMetadata metadata() {
      return ((UnfilteredPartitionIterator)this.input).metadata();
   }
}
