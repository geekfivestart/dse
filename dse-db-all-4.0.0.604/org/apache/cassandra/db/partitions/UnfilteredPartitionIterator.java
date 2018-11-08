package org.apache.cassandra.db.partitions;

import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.TableMetadata;

public interface UnfilteredPartitionIterator extends BasePartitionIterator<UnfilteredRowIterator> {
   TableMetadata metadata();

   void close();
}
