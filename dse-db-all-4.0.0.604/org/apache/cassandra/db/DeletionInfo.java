package org.apache.cassandra.db;

import java.util.Iterator;
import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.utils.memory.AbstractAllocator;

public interface DeletionInfo extends IMeasurableMemory {
   DeletionInfo LIVE = MutableDeletionInfo.live();

   boolean isLive();

   DeletionTime getPartitionDeletion();

   Iterator<RangeTombstone> rangeIterator(boolean var1);

   Iterator<RangeTombstone> rangeIterator(Slice var1, boolean var2);

   RangeTombstone rangeCovering(Clustering var1);

   void collectStats(EncodingStats.Collector var1);

   int dataSize();

   boolean hasRanges();

   int rangeCount();

   long maxTimestamp();

   boolean mayModify(DeletionInfo var1);

   MutableDeletionInfo mutableCopy();

   DeletionInfo copy(AbstractAllocator var1);
}
