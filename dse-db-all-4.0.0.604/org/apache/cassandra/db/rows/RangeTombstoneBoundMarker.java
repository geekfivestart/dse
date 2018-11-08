package org.apache.cassandra.db.rows;

import com.google.common.hash.Hasher;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.memory.AbstractAllocator;

public class RangeTombstoneBoundMarker extends AbstractRangeTombstoneMarker<ClusteringBound> {
   private final DeletionTime deletion;

   public RangeTombstoneBoundMarker(ClusteringBound bound, DeletionTime deletion) {
      super(bound);
      this.deletion = deletion;
   }

   public static RangeTombstoneBoundMarker inclusiveOpen(boolean reversed, ByteBuffer[] boundValues, DeletionTime deletion) {
      ClusteringBound bound = ClusteringBound.inclusiveOpen(reversed, boundValues);
      return new RangeTombstoneBoundMarker(bound, deletion);
   }

   public static RangeTombstoneBoundMarker exclusiveOpen(boolean reversed, ByteBuffer[] boundValues, DeletionTime deletion) {
      ClusteringBound bound = ClusteringBound.exclusiveOpen(reversed, boundValues);
      return new RangeTombstoneBoundMarker(bound, deletion);
   }

   public static RangeTombstoneBoundMarker inclusiveClose(boolean reversed, ByteBuffer[] boundValues, DeletionTime deletion) {
      ClusteringBound bound = ClusteringBound.inclusiveClose(reversed, boundValues);
      return new RangeTombstoneBoundMarker(bound, deletion);
   }

   public static RangeTombstoneBoundMarker exclusiveClose(boolean reversed, ByteBuffer[] boundValues, DeletionTime deletion) {
      ClusteringBound bound = ClusteringBound.exclusiveClose(reversed, boundValues);
      return new RangeTombstoneBoundMarker(bound, deletion);
   }

   public boolean isBoundary() {
      return false;
   }

   public DeletionTime deletionTime() {
      return this.deletion;
   }

   public DeletionTime openDeletionTime(boolean reversed) {
      if(!this.isOpen(reversed)) {
         throw new IllegalStateException();
      } else {
         return this.deletion;
      }
   }

   public DeletionTime closeDeletionTime(boolean reversed) {
      if(this.isOpen(reversed)) {
         throw new IllegalStateException();
      } else {
         return this.deletion;
      }
   }

   public boolean openIsInclusive(boolean reversed) {
      if(!this.isOpen(reversed)) {
         throw new IllegalStateException();
      } else {
         return ((ClusteringBound)this.bound).isInclusive();
      }
   }

   public boolean closeIsInclusive(boolean reversed) {
      if(this.isOpen(reversed)) {
         throw new IllegalStateException();
      } else {
         return ((ClusteringBound)this.bound).isInclusive();
      }
   }

   public ClusteringBound openBound(boolean reversed) {
      return this.isOpen(reversed)?(ClusteringBound)this.clustering():null;
   }

   public ClusteringBound closeBound(boolean reversed) {
      return this.isClose(reversed)?(ClusteringBound)this.clustering():null;
   }

   public RangeTombstoneBoundMarker copy(AbstractAllocator allocator) {
      return new RangeTombstoneBoundMarker(((ClusteringBound)this.clustering()).copy(allocator), this.deletion);
   }

   public RangeTombstoneBoundMarker withNewOpeningDeletionTime(boolean reversed, DeletionTime newDeletionTime) {
      if(!this.isOpen(reversed)) {
         throw new IllegalStateException();
      } else {
         return new RangeTombstoneBoundMarker((ClusteringBound)this.clustering(), newDeletionTime);
      }
   }

   public void digest(Hasher hasher) {
      ((ClusteringBound)this.bound).digest(hasher);
      this.deletion.digest(hasher);
   }

   public RangeTombstoneMarker purge(DeletionPurger purger, int nowInSec, boolean enforceStrictLiveness) {
      return purger.shouldPurge(this.deletionTime())?null:this;
   }

   public String toString(TableMetadata metadata) {
      return String.format("Marker %s@%d/%d", new Object[]{((ClusteringBound)this.bound).toString(metadata), Long.valueOf(this.deletion.markedForDeleteAt()), Integer.valueOf(this.deletion.localDeletionTime())});
   }

   public boolean equals(Object other) {
      if(!(other instanceof RangeTombstoneBoundMarker)) {
         return false;
      } else {
         RangeTombstoneBoundMarker that = (RangeTombstoneBoundMarker)other;
         return ((ClusteringBound)this.bound).equals(that.bound) && this.deletion.equals(that.deletion);
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.bound, this.deletion});
   }
}
