package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.memory.AbstractAllocator;

public class ClusteringBound extends ClusteringBoundOrBoundary {
   public static final ClusteringBound BOTTOM;
   public static final ClusteringBound TOP;

   protected ClusteringBound(ClusteringPrefix.Kind kind, ByteBuffer[] values) {
      super(kind, values);
   }

   public static ClusteringBound create(ClusteringPrefix.Kind kind, ByteBuffer[] values) {
      assert !kind.isBoundary();

      return new ClusteringBound(kind, values);
   }

   public static ClusteringPrefix.Kind boundKind(boolean isStart, boolean isInclusive) {
      return isStart?(isInclusive?ClusteringPrefix.Kind.INCL_START_BOUND:ClusteringPrefix.Kind.EXCL_START_BOUND):(isInclusive?ClusteringPrefix.Kind.INCL_END_BOUND:ClusteringPrefix.Kind.EXCL_END_BOUND);
   }

   public static ClusteringBound inclusiveStartOf(ByteBuffer... values) {
      return create(ClusteringPrefix.Kind.INCL_START_BOUND, values);
   }

   public static ClusteringBound inclusiveEndOf(ByteBuffer... values) {
      return create(ClusteringPrefix.Kind.INCL_END_BOUND, values);
   }

   public static ClusteringBound exclusiveStartOf(ByteBuffer... values) {
      return create(ClusteringPrefix.Kind.EXCL_START_BOUND, values);
   }

   public static ClusteringBound exclusiveEndOf(ByteBuffer... values) {
      return create(ClusteringPrefix.Kind.EXCL_END_BOUND, values);
   }

   public static ClusteringBound inclusiveStartOf(ClusteringPrefix prefix) {
      ByteBuffer[] values = new ByteBuffer[prefix.size()];

      for(int i = 0; i < prefix.size(); ++i) {
         values[i] = prefix.get(i);
      }

      return inclusiveStartOf(values);
   }

   public static ClusteringBound exclusiveStartOf(ClusteringPrefix prefix) {
      ByteBuffer[] values = new ByteBuffer[prefix.size()];

      for(int i = 0; i < prefix.size(); ++i) {
         values[i] = prefix.get(i);
      }

      return exclusiveStartOf(values);
   }

   public static ClusteringBound inclusiveEndOf(ClusteringPrefix prefix) {
      ByteBuffer[] values = new ByteBuffer[prefix.size()];

      for(int i = 0; i < prefix.size(); ++i) {
         values[i] = prefix.get(i);
      }

      return inclusiveEndOf(values);
   }

   public static ClusteringBound create(ClusteringComparator comparator, boolean isStart, boolean isInclusive, Object... values) {
      CBuilder builder = CBuilder.create(comparator);
      Object[] var5 = values;
      int var6 = values.length;

      for(int var7 = 0; var7 < var6; ++var7) {
         Object val = var5[var7];
         if(val instanceof ByteBuffer) {
            builder.add((ByteBuffer)val);
         } else {
            builder.add(val);
         }
      }

      return builder.buildBound(isStart, isInclusive);
   }

   public ClusteringBound invert() {
      return create(this.kind().invert(), this.values);
   }

   public ClusteringBound copy(AbstractAllocator allocator) {
      return (ClusteringBound)super.copy(allocator);
   }

   public boolean isStart() {
      return this.kind().isStart();
   }

   public boolean isEnd() {
      return !this.isStart();
   }

   public boolean isInclusive() {
      return this.kind == ClusteringPrefix.Kind.INCL_START_BOUND || this.kind == ClusteringPrefix.Kind.INCL_END_BOUND;
   }

   public boolean isExclusive() {
      return this.kind == ClusteringPrefix.Kind.EXCL_START_BOUND || this.kind == ClusteringPrefix.Kind.EXCL_END_BOUND;
   }

   int compareTo(ClusteringComparator comparator, List<ByteBuffer> sstableBound) {
      for(int i = 0; i < sstableBound.size(); ++i) {
         if(i >= this.size()) {
            return this.isStart()?-1:1;
         }

         int cmp = comparator.compareComponent(i, this.get(i), (ByteBuffer)sstableBound.get(i));
         if(cmp != 0) {
            return cmp;
         }
      }

      if(this.size() > sstableBound.size()) {
         return this.isStart()?-1:1;
      } else {
         return this.isInclusive()?0:(this.isStart()?1:-1);
      }
   }

   static {
      BOTTOM = new ClusteringBound(ClusteringPrefix.Kind.INCL_START_BOUND, ByteBufferUtil.EMPTY_BUFFER_ARRAY);
      TOP = new ClusteringBound(ClusteringPrefix.Kind.INCL_END_BOUND, ByteBufferUtil.EMPTY_BUFFER_ARRAY);
   }
}
