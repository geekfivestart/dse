package org.apache.cassandra.db;

import java.net.InetAddress;
import java.util.Collection;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;

public interface ReadReconciliationObserver {
   void queried(Collection<InetAddress> var1);

   void responsesReceived(Collection<InetAddress> var1);

   void onDigestMatch();

   void onDigestMismatch();

   void onPartition(DecoratedKey var1);

   void onPartitionDeletion(DeletionTime var1, boolean var2);

   void onRow(Row var1, boolean var2);

   void onRangeTombstoneMarker(RangeTombstoneMarker var1, boolean var2);

   void onRepair(InetAddress var1, PartitionUpdate var2);
}
