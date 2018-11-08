package org.apache.cassandra.db.rows;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringBoundOrBoundary;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.utils.memory.AbstractAllocator;

public interface RangeTombstoneMarker extends Unfiltered {
   ClusteringBoundOrBoundary clustering();

   boolean isBoundary();

   boolean isOpen(boolean var1);

   boolean isClose(boolean var1);

   DeletionTime openDeletionTime(boolean var1);

   DeletionTime closeDeletionTime(boolean var1);

   boolean openIsInclusive(boolean var1);

   boolean closeIsInclusive(boolean var1);

   ClusteringBound openBound(boolean var1);

   ClusteringBound closeBound(boolean var1);

   RangeTombstoneMarker copy(AbstractAllocator var1);

   default boolean isEmpty() {
      return false;
   }

   RangeTombstoneMarker purge(DeletionPurger var1, int var2, boolean var3);

   public static class Merger {
      private final DeletionTime partitionDeletion;
      private final boolean reversed;
      private ClusteringBoundOrBoundary bound;
      private final RangeTombstoneMarker[] markers;
      private final DeletionTime[] openMarkers;
      private int biggestOpenMarker = -1;

      public Merger(int size, DeletionTime partitionDeletion, boolean reversed) {
         this.partitionDeletion = partitionDeletion;
         this.reversed = reversed;
         this.markers = new RangeTombstoneMarker[size];
         this.openMarkers = new DeletionTime[size];
      }

      public void clear() {
         Arrays.fill(this.markers, (Object)null);
      }

      public void add(int i, RangeTombstoneMarker marker) {
         this.bound = marker.clustering();
         this.markers[i] = marker;
      }

      public RangeTombstoneMarker merge() {
         DeletionTime previousDeletionTimeInMerged = this.currentOpenDeletionTimeInMerged();
         this.updateOpenMarkers();
         DeletionTime newDeletionTimeInMerged = this.currentOpenDeletionTimeInMerged();
         if(previousDeletionTimeInMerged.equals(newDeletionTimeInMerged)) {
            return null;
         } else {
            boolean isBeforeClustering = this.bound.kind().comparedToClustering < 0;
            if(this.reversed) {
               isBeforeClustering = !isBeforeClustering;
            }

            ByteBuffer[] values = this.bound.getRawValues();
            Object merged;
            if(previousDeletionTimeInMerged.isLive()) {
               merged = isBeforeClustering?RangeTombstoneBoundMarker.inclusiveOpen(this.reversed, values, newDeletionTimeInMerged):RangeTombstoneBoundMarker.exclusiveOpen(this.reversed, values, newDeletionTimeInMerged);
            } else if(newDeletionTimeInMerged.isLive()) {
               merged = isBeforeClustering?RangeTombstoneBoundMarker.exclusiveClose(this.reversed, values, previousDeletionTimeInMerged):RangeTombstoneBoundMarker.inclusiveClose(this.reversed, values, previousDeletionTimeInMerged);
            } else {
               merged = isBeforeClustering?RangeTombstoneBoundaryMarker.exclusiveCloseInclusiveOpen(this.reversed, values, previousDeletionTimeInMerged, newDeletionTimeInMerged):RangeTombstoneBoundaryMarker.inclusiveCloseExclusiveOpen(this.reversed, values, previousDeletionTimeInMerged, newDeletionTimeInMerged);
            }

            return (RangeTombstoneMarker)merged;
         }
      }

      public RangeTombstoneMarker[] mergedMarkers() {
         return this.markers;
      }

      private DeletionTime currentOpenDeletionTimeInMerged() {
         if(this.biggestOpenMarker < 0) {
            return DeletionTime.LIVE;
         } else {
            DeletionTime biggestDeletionTime = this.openMarkers[this.biggestOpenMarker];
            return this.partitionDeletion.supersedes(biggestDeletionTime)?DeletionTime.LIVE:biggestDeletionTime;
         }
      }

      private void updateOpenMarkers() {
         int i;
         for(i = 0; i < this.markers.length; ++i) {
            RangeTombstoneMarker marker = this.markers[i];
            if(marker != null) {
               if(marker.isOpen(this.reversed)) {
                  this.openMarkers[i] = marker.openDeletionTime(this.reversed);
               } else {
                  this.openMarkers[i] = null;
               }
            }
         }

         this.biggestOpenMarker = -1;

         for(i = 0; i < this.openMarkers.length; ++i) {
            if(this.openMarkers[i] != null && (this.biggestOpenMarker < 0 || this.openMarkers[i].supersedes(this.openMarkers[this.biggestOpenMarker]))) {
               this.biggestOpenMarker = i;
            }
         }

      }

      public DeletionTime activeDeletion() {
         DeletionTime openMarker = this.currentOpenDeletionTimeInMerged();
         return openMarker.isLive()?this.partitionDeletion:openMarker;
      }
   }
}
