package org.apache.cassandra.db.transform;

import org.apache.cassandra.db.partitions.BasePartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;

public interface MorePartitions<I extends BasePartitionIterator<?>> extends MoreContents<I> {
   public static UnfilteredPartitionIterator extend(UnfilteredPartitionIterator iterator, MorePartitions<? super UnfilteredPartitionIterator> more) {
      return (UnfilteredPartitionIterator)Transformation.add(Transformation.mutable(iterator), (MoreContents)more);
   }

   public static PartitionIterator extend(PartitionIterator iterator, MorePartitions<? super PartitionIterator> more) {
      return (PartitionIterator)Transformation.add(Transformation.mutable(iterator), (MoreContents)more);
   }
}
