package org.apache.cassandra.db.partitions;

import java.util.function.LongPredicate;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.transform.Transformation;

public abstract class PurgeFunction extends Transformation<UnfilteredRowIterator> {
   private final DeletionPurger purger;
   private final int nowInSec;
   private final boolean enforceStrictLiveness;

   public PurgeFunction(int nowInSec, int gcBefore, int oldestUnrepairedTombstone, boolean onlyPurgeRepairedTombstones, boolean enforceStrictLiveness) {
      this.nowInSec = nowInSec;
      this.purger = (timestamp, localDeletionTime) -> {
         return (!onlyPurgeRepairedTombstones || localDeletionTime < oldestUnrepairedTombstone) && localDeletionTime < gcBefore && this.getPurgeEvaluator().test(timestamp);
      };
      this.enforceStrictLiveness = enforceStrictLiveness;
   }

   protected abstract LongPredicate getPurgeEvaluator();

   protected void onNewPartition(DecoratedKey partitionKey) {
   }

   protected void onEmptyPartitionPostPurge(DecoratedKey partitionKey) {
   }

   protected void updateProgress() {
   }

   protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition) {
      this.onNewPartition(partition.partitionKey());
      UnfilteredRowIterator purged = Transformation.apply((UnfilteredRowIterator)partition, this);
      if(purged.isEmpty()) {
         this.onEmptyPartitionPostPurge(purged.partitionKey());
         purged.close();
         return null;
      } else {
         return purged;
      }
   }

   protected DeletionTime applyToDeletion(DeletionTime deletionTime) {
      return this.purger.shouldPurge(deletionTime)?DeletionTime.LIVE:deletionTime;
   }

   public Row applyToStatic(Row row) {
      this.updateProgress();
      return row.purge(this.purger, this.nowInSec, this.enforceStrictLiveness);
   }

   protected Row applyToRow(Row row) {
      this.updateProgress();
      return row.purge(this.purger, this.nowInSec, this.enforceStrictLiveness);
   }

   protected RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker) {
      this.updateProgress();
      return marker.purge(this.purger, this.nowInSec, this.enforceStrictLiveness);
   }
}
