package org.apache.cassandra.db.partitions;

import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;

public interface PartitionStatisticsCollector {
   void update(LivenessInfo var1);

   void update(DeletionTime var1);

   void update(Cell var1);

   void update(ColumnData var1);

   void updateHasLegacyCounterShards(boolean var1);

   void updateRowStats();
}
