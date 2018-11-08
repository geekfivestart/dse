package org.apache.cassandra.db.rows;

import com.google.common.hash.Hasher;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringBoundary;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.memory.AbstractAllocator;

public class RangeTombstoneBoundaryMarker extends AbstractRangeTombstoneMarker<ClusteringBoundary> {
   private final DeletionTime endDeletion;
   private final DeletionTime startDeletion;

   public RangeTombstoneBoundaryMarker(ClusteringBoundary bound, DeletionTime endDeletion, DeletionTime startDeletion) {
      super(bound);

      assert bound.isBoundary();

      this.endDeletion = endDeletion;
      this.startDeletion = startDeletion;
   }

   public static RangeTombstoneBoundaryMarker exclusiveCloseInclusiveOpen(boolean reversed, ByteBuffer[] boundValues, DeletionTime closeDeletion, DeletionTime openDeletion) {
      ClusteringBoundary bound = ClusteringBoundary.exclusiveCloseInclusiveOpen(reversed, boundValues);
      DeletionTime endDeletion = reversed?openDeletion:closeDeletion;
      DeletionTime startDeletion = reversed?closeDeletion:openDeletion;
      return new RangeTombstoneBoundaryMarker(bound, endDeletion, startDeletion);
   }

   public static RangeTombstoneBoundaryMarker inclusiveCloseExclusiveOpen(boolean reversed, ByteBuffer[] boundValues, DeletionTime closeDeletion, DeletionTime openDeletion) {
      ClusteringBoundary bound = ClusteringBoundary.inclusiveCloseExclusiveOpen(reversed, boundValues);
      DeletionTime endDeletion = reversed?openDeletion:closeDeletion;
      DeletionTime startDeletion = reversed?closeDeletion:openDeletion;
      return new RangeTombstoneBoundaryMarker(bound, endDeletion, startDeletion);
   }

   public DeletionTime endDeletionTime() {
      return this.endDeletion;
   }

   public DeletionTime startDeletionTime() {
      return this.startDeletion;
   }

   public DeletionTime closeDeletionTime(boolean reversed) {
      return reversed?this.startDeletion:this.endDeletion;
   }

   public DeletionTime openDeletionTime(boolean reversed) {
      return reversed?this.endDeletion:this.startDeletion;
   }

   public boolean openIsInclusive(boolean reversed) {
      return ((ClusteringBoundary)this.bound).kind() == ClusteringPrefix.Kind.EXCL_END_INCL_START_BOUNDARY ^ reversed;
   }

   public ClusteringBound openBound(boolean reversed) {
      return ((ClusteringBoundary)this.bound).openBound(reversed);
   }

   public ClusteringBound closeBound(boolean reversed) {
      return ((ClusteringBoundary)this.bound).closeBound(reversed);
   }

   public boolean closeIsInclusive(boolean reversed) {
      return ((ClusteringBoundary)this.bound).kind() == ClusteringPrefix.Kind.INCL_END_EXCL_START_BOUNDARY ^ reversed;
   }

   public boolean isOpen(boolean reversed) {
      return true;
   }

   public boolean isClose(boolean reversed) {
      return true;
   }

   public RangeTombstoneBoundaryMarker copy(AbstractAllocator allocator) {
      return new RangeTombstoneBoundaryMarker(((ClusteringBoundary)this.clustering()).copy(allocator), this.endDeletion, this.startDeletion);
   }

   public RangeTombstoneBoundaryMarker withNewOpeningDeletionTime(boolean reversed, DeletionTime newDeletionTime) {
      return new RangeTombstoneBoundaryMarker((ClusteringBoundary)this.clustering(), reversed?newDeletionTime:this.endDeletion, reversed?this.startDeletion:newDeletionTime);
   }

   public static RangeTombstoneBoundaryMarker makeBoundary(boolean reversed, ClusteringBound close, ClusteringBound open, DeletionTime closeDeletion, DeletionTime openDeletion) {
      assert ClusteringPrefix.Kind.compare(close.kind(), open.kind()) == 0 : "Both bound don't form a boundary";

      boolean isExclusiveClose = close.isExclusive() || close.isInclusive() && open.isInclusive() && openDeletion.supersedes(closeDeletion);
      return isExclusiveClose?exclusiveCloseInclusiveOpen(reversed, close.getRawValues(), closeDeletion, openDeletion):inclusiveCloseExclusiveOpen(reversed, close.getRawValues(), closeDeletion, openDeletion);
   }

   public RangeTombstoneBoundMarker createCorrespondingCloseMarker(boolean reversed) {
      return new RangeTombstoneBoundMarker(this.closeBound(reversed), this.endDeletion);
   }

   public RangeTombstoneBoundMarker createCorrespondingOpenMarker(boolean reversed) {
      return new RangeTombstoneBoundMarker(this.openBound(reversed), this.startDeletion);
   }

   public void digest(Hasher hasher) {
      ((ClusteringBoundary)this.bound).digest(hasher);
      this.endDeletion.digest(hasher);
      this.startDeletion.digest(hasher);
   }

   public RangeTombstoneMarker purge(DeletionPurger purger, int nowInSec, boolean enforceStrictLiveness) {
      boolean shouldPurgeEnd = purger.shouldPurge(this.endDeletion);
      boolean shouldPurgeStart = purger.shouldPurge(this.startDeletion);
      return (RangeTombstoneMarker)(shouldPurgeEnd?(shouldPurgeStart?null:this.createCorrespondingOpenMarker(false)):(shouldPurgeStart?this.createCorrespondingCloseMarker(false):this));
   }

   public String toString(TableMetadata metadata) {
      return String.format("Marker %s@%d/%d-%d/%d", new Object[]{((ClusteringBoundary)this.bound).toString(metadata), Long.valueOf(this.endDeletion.markedForDeleteAt()), Integer.valueOf(this.endDeletion.localDeletionTime()), Long.valueOf(this.startDeletion.markedForDeleteAt()), Integer.valueOf(this.startDeletion.localDeletionTime())});
   }

   public boolean equals(Object other) {
      if(!(other instanceof RangeTombstoneBoundaryMarker)) {
         return false;
      } else {
         RangeTombstoneBoundaryMarker that = (RangeTombstoneBoundaryMarker)other;
         return ((ClusteringBoundary)this.bound).equals(that.bound) && this.endDeletion.equals(that.endDeletion) && this.startDeletion.equals(that.startDeletion);
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.bound, this.endDeletion, this.startDeletion});
   }
}
