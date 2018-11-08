package org.apache.cassandra.db.rows;

public interface UnfilteredRowIterator extends PartitionTrait, BaseRowIterator<Unfiltered> {
   default boolean isEmpty() {
      return this.partitionLevelDeletion().isLive() && this.staticRow().isEmpty() && !this.hasNext();
   }
}
