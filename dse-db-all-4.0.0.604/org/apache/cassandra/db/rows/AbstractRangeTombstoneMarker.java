package org.apache.cassandra.db.rows;

import java.nio.ByteBuffer;
import org.apache.cassandra.db.ClusteringBoundOrBoundary;
import org.apache.cassandra.schema.TableMetadata;

public abstract class AbstractRangeTombstoneMarker<B extends ClusteringBoundOrBoundary> implements RangeTombstoneMarker {
   protected final B bound;

   protected AbstractRangeTombstoneMarker(B bound) {
      this.bound = bound;
   }

   public B clustering() {
      return this.bound;
   }

   public Unfiltered.Kind kind() {
      return Unfiltered.Kind.RANGE_TOMBSTONE_MARKER;
   }

   public boolean isBoundary() {
      return this.bound.isBoundary();
   }

   public boolean isOpen(boolean reversed) {
      return this.bound.isOpen(reversed);
   }

   public boolean isClose(boolean reversed) {
      return this.bound.isClose(reversed);
   }

   public void validateData(TableMetadata metadata) {
      ClusteringBoundOrBoundary bound = this.clustering();

      for(int i = 0; i < bound.size(); ++i) {
         ByteBuffer value = bound.get(i);
         if(value != null) {
            metadata.comparator.subtype(i).validate(value);
         }
      }

   }

   public String toString(TableMetadata metadata, boolean fullDetails) {
      return this.toString(metadata);
   }

   public String toString(TableMetadata metadata, boolean includeClusteringKeys, boolean fullDetails) {
      return this.toString(metadata);
   }
}
