package org.apache.cassandra.db.partitions;

import org.apache.cassandra.cache.IRowCacheEntry;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.ISerializer;

public interface CachedPartition extends Partition, IRowCacheEntry {
   ISerializer<CachedPartition> cacheSerializer = new CachedBTreePartition.Serializer();

   int rowCount();

   int cachedLiveRows();

   int rowsWithNonExpiringCells();

   Row lastRow();
}
