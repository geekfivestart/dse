package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import org.apache.cassandra.utils.ObjectSizes;

public abstract class AbstractBufferClusteringPrefix extends AbstractClusteringPrefix {
   protected final ClusteringPrefix.Kind kind;
   protected final ByteBuffer[] values;

   protected AbstractBufferClusteringPrefix(ClusteringPrefix.Kind kind, ByteBuffer[] values) {
      this.kind = kind;
      this.values = values;
   }

   public ClusteringPrefix.Kind kind() {
      return this.kind;
   }

   public ClusteringPrefix clustering() {
      return this;
   }

   public int size() {
      return this.values.length;
   }

   public ByteBuffer get(int i) {
      return this.values[i];
   }

   public ByteBuffer[] getRawValues() {
      return this.values;
   }

   public long unsharedHeapSize() {
      return Clustering.EMPTY_SIZE + ObjectSizes.sizeOnHeapOf(this.values);
   }

   public long unsharedHeapSizeExcludingData() {
      return Clustering.EMPTY_SIZE + ObjectSizes.sizeOnHeapExcludingData(this.values);
   }
}
