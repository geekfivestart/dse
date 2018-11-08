package org.apache.cassandra.db.partitions;

import org.apache.cassandra.db.rows.RowIterator;

public interface PartitionIterator extends BasePartitionIterator<RowIterator> {
   void close();
}
