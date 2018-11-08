package org.apache.cassandra.db.transform;

import org.apache.cassandra.db.partitions.BasePartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.RowIterator;

public final class FilteredPartitions extends BasePartitions<RowIterator, BasePartitionIterator<?>> implements PartitionIterator {
   FilteredPartitions(PartitionIterator input) {
      super((BasePartitionIterator)input);
   }

   FilteredPartitions(UnfilteredPartitionIterator input, Filter filter) {
      super((BasePartitionIterator)input);
      this.add(filter);
   }

   FilteredPartitions(Filter filter, UnfilteredPartitions copyFrom) {
      super((BasePartitions)copyFrom);
      this.add(filter);
   }

   public static FilteredPartitions filter(UnfilteredPartitionIterator iterator, int nowInSecs) {
      FilteredPartitions filtered = filter(iterator, new Filter(nowInSecs, iterator.metadata().enforceStrictLiveness()));
      return (FilteredPartitions)Transformation.apply((PartitionIterator)filtered, new EmptyPartitionsDiscarder());
   }

   public static FilteredPartitions filter(UnfilteredPartitionIterator iterator, Filter filter) {
      return iterator instanceof UnfilteredPartitions?new FilteredPartitions(filter, (UnfilteredPartitions)iterator):new FilteredPartitions(iterator, filter);
   }
}
