package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import org.apache.cassandra.utils.memory.AbstractAllocator;

public class ClusteringBoundary extends ClusteringBoundOrBoundary {
   protected ClusteringBoundary(ClusteringPrefix.Kind kind, ByteBuffer[] values) {
      super(kind, values);
   }

   public static ClusteringBoundary create(ClusteringPrefix.Kind kind, ByteBuffer[] values) {
      assert kind.isBoundary();

      return new ClusteringBoundary(kind, values);
   }

   public ClusteringBoundary invert() {
      return create(this.kind().invert(), this.values);
   }

   public ClusteringBoundary copy(AbstractAllocator allocator) {
      return (ClusteringBoundary)super.copy(allocator);
   }

   public ClusteringBound openBound(boolean reversed) {
      return ClusteringBound.create(this.kind.openBoundOfBoundary(reversed), this.values);
   }

   public ClusteringBound closeBound(boolean reversed) {
      return ClusteringBound.create(this.kind.closeBoundOfBoundary(reversed), this.values);
   }
}
