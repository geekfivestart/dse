package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public abstract class CBuilder {
   public static CBuilder STATIC_BUILDER = new CBuilder() {
      public int count() {
         return 0;
      }

      public int remainingCount() {
         return 0;
      }

      public ClusteringComparator comparator() {
         throw new UnsupportedOperationException();
      }

      public CBuilder add(ByteBuffer value) {
         throw new UnsupportedOperationException();
      }

      public CBuilder add(Object value) {
         throw new UnsupportedOperationException();
      }

      public Clustering build() {
         return Clustering.STATIC_CLUSTERING;
      }

      public ClusteringBound buildBound(boolean isStart, boolean isInclusive) {
         throw new UnsupportedOperationException();
      }

      public Slice buildSlice() {
         throw new UnsupportedOperationException();
      }

      public Clustering buildWith(ByteBuffer value) {
         throw new UnsupportedOperationException();
      }

      public Clustering buildWith(List<ByteBuffer> newValues) {
         throw new UnsupportedOperationException();
      }

      public ClusteringBound buildBoundWith(ByteBuffer value, boolean isStart, boolean isInclusive) {
         throw new UnsupportedOperationException();
      }

      public ClusteringBound buildBoundWith(List<ByteBuffer> newValues, boolean isStart, boolean isInclusive) {
         throw new UnsupportedOperationException();
      }
   };

   public CBuilder() {
   }

   public static CBuilder create(ClusteringComparator comparator) {
      return new CBuilder.ArrayBackedBuilder(comparator);
   }

   public abstract int count();

   public abstract int remainingCount();

   public abstract ClusteringComparator comparator();

   public abstract CBuilder add(ByteBuffer var1);

   public abstract CBuilder add(Object var1);

   public abstract Clustering build();

   public abstract ClusteringBound buildBound(boolean var1, boolean var2);

   public abstract Slice buildSlice();

   public abstract Clustering buildWith(ByteBuffer var1);

   public abstract Clustering buildWith(List<ByteBuffer> var1);

   public abstract ClusteringBound buildBoundWith(ByteBuffer var1, boolean var2, boolean var3);

   public abstract ClusteringBound buildBoundWith(List<ByteBuffer> var1, boolean var2, boolean var3);

   private static class ArrayBackedBuilder extends CBuilder {
      private final ClusteringComparator type;
      private final ByteBuffer[] values;
      private int size;
      private boolean built;

      public ArrayBackedBuilder(ClusteringComparator type) {
         this.type = type;
         this.values = new ByteBuffer[type.size()];
      }

      public int count() {
         return this.size;
      }

      public int remainingCount() {
         return this.values.length - this.size;
      }

      public ClusteringComparator comparator() {
         return this.type;
      }

      public CBuilder add(ByteBuffer value) {
         if(this.isDone()) {
            throw new IllegalStateException();
         } else {
            this.values[this.size++] = value;
            return this;
         }
      }

      public CBuilder add(Object value) {
         return this.add(this.type.subtype(this.size).decompose(value));
      }

      private boolean isDone() {
         return this.remainingCount() == 0 || this.built;
      }

      public Clustering build() {
         this.built = true;
         return this.size == 0?Clustering.EMPTY:Clustering.make(this.values);
      }

      public ClusteringBound buildBound(boolean isStart, boolean isInclusive) {
         this.built = true;
         return this.size == 0?(isStart?ClusteringBound.BOTTOM:ClusteringBound.TOP):ClusteringBound.create(ClusteringBound.boundKind(isStart, isInclusive), this.size == this.values.length?this.values:(ByteBuffer[])Arrays.copyOfRange(this.values, 0, this.size));
      }

      public Slice buildSlice() {
         this.built = true;
         return this.size == 0?Slice.ALL:Slice.make(this.buildBound(true, true), this.buildBound(false, true));
      }

      public Clustering buildWith(ByteBuffer value) {
         assert this.size + 1 <= this.type.size();

         ByteBuffer[] newValues = (ByteBuffer[])Arrays.copyOf(this.values, this.type.size());
         newValues[this.size] = value;
         return Clustering.make(newValues);
      }

      public Clustering buildWith(List<ByteBuffer> newValues) {
         assert this.size + newValues.size() <= this.type.size();

         ByteBuffer[] buffers = (ByteBuffer[])Arrays.copyOf(this.values, this.type.size());
         int newSize = this.size;

         ByteBuffer value;
         for(Iterator var4 = newValues.iterator(); var4.hasNext(); buffers[newSize++] = value) {
            value = (ByteBuffer)var4.next();
         }

         return Clustering.make(buffers);
      }

      public ClusteringBound buildBoundWith(ByteBuffer value, boolean isStart, boolean isInclusive) {
         ByteBuffer[] newValues = (ByteBuffer[])Arrays.copyOf(this.values, this.size + 1);
         newValues[this.size] = value;
         return ClusteringBound.create(ClusteringBound.boundKind(isStart, isInclusive), newValues);
      }

      public ClusteringBound buildBoundWith(List<ByteBuffer> newValues, boolean isStart, boolean isInclusive) {
         ByteBuffer[] buffers = (ByteBuffer[])Arrays.copyOf(this.values, this.size + newValues.size());
         int newSize = this.size;

         ByteBuffer value;
         for(Iterator var6 = newValues.iterator(); var6.hasNext(); buffers[newSize++] = value) {
            value = (ByteBuffer)var6.next();
         }

         return ClusteringBound.create(ClusteringBound.boundKind(isStart, isInclusive), buffers);
      }
   }
}
