package org.apache.cassandra.db.rows;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.schema.TableMetadata;

public interface PartitionTrait {
   TableMetadata metadata();

   boolean isReverseOrder();

   RegularAndStaticColumns columns();

   DecoratedKey partitionKey();

   Row staticRow();

   DeletionTime partitionLevelDeletion();

   EncodingStats stats();
}
