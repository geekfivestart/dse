package org.apache.cassandra.db.partitions;

public abstract class AbstractUnfilteredPartitionIterator implements UnfilteredPartitionIterator {
   public AbstractUnfilteredPartitionIterator() {
   }

   public void remove() {
      throw new UnsupportedOperationException();
   }
}
