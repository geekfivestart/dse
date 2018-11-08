package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.memory.AbstractAllocator;

public abstract class ClusteringBoundOrBoundary extends AbstractBufferClusteringPrefix {
   public static final ClusteringBoundOrBoundary.Serializer serializer = new ClusteringBoundOrBoundary.Serializer();

   protected ClusteringBoundOrBoundary(ClusteringPrefix.Kind kind, ByteBuffer[] values) {
      super(kind, values);

      assert values.length > 0 || !kind.isBoundary();

   }

   public static ClusteringBoundOrBoundary create(ClusteringPrefix.Kind kind, ByteBuffer[] values) {
      return (ClusteringBoundOrBoundary)(kind.isBoundary()?new ClusteringBoundary(kind, values):new ClusteringBound(kind, values));
   }

   public boolean isBoundary() {
      return this.kind.isBoundary();
   }

   public boolean isOpen(boolean reversed) {
      return this.kind.isOpen(reversed);
   }

   public boolean isClose(boolean reversed) {
      return this.kind.isClose(reversed);
   }

   public static ClusteringBound inclusiveOpen(boolean reversed, ByteBuffer[] boundValues) {
      return new ClusteringBound(reversed?ClusteringPrefix.Kind.INCL_END_BOUND:ClusteringPrefix.Kind.INCL_START_BOUND, boundValues);
   }

   public static ClusteringBound exclusiveOpen(boolean reversed, ByteBuffer[] boundValues) {
      return new ClusteringBound(reversed?ClusteringPrefix.Kind.EXCL_END_BOUND:ClusteringPrefix.Kind.EXCL_START_BOUND, boundValues);
   }

   public static ClusteringBound inclusiveClose(boolean reversed, ByteBuffer[] boundValues) {
      return new ClusteringBound(reversed?ClusteringPrefix.Kind.INCL_START_BOUND:ClusteringPrefix.Kind.INCL_END_BOUND, boundValues);
   }

   public static ClusteringBound exclusiveClose(boolean reversed, ByteBuffer[] boundValues) {
      return new ClusteringBound(reversed?ClusteringPrefix.Kind.EXCL_START_BOUND:ClusteringPrefix.Kind.EXCL_END_BOUND, boundValues);
   }

   public static ClusteringBoundary inclusiveCloseExclusiveOpen(boolean reversed, ByteBuffer[] boundValues) {
      return new ClusteringBoundary(reversed?ClusteringPrefix.Kind.EXCL_END_INCL_START_BOUNDARY:ClusteringPrefix.Kind.INCL_END_EXCL_START_BOUNDARY, boundValues);
   }

   public static ClusteringBoundary exclusiveCloseInclusiveOpen(boolean reversed, ByteBuffer[] boundValues) {
      return new ClusteringBoundary(reversed?ClusteringPrefix.Kind.INCL_END_EXCL_START_BOUNDARY:ClusteringPrefix.Kind.EXCL_END_INCL_START_BOUNDARY, boundValues);
   }

   public ClusteringBoundOrBoundary copy(AbstractAllocator allocator) {
      ByteBuffer[] newValues = new ByteBuffer[this.size()];

      for(int i = 0; i < this.size(); ++i) {
         newValues[i] = allocator.clone(this.get(i));
      }

      return create(this.kind(), newValues);
   }

   public String toString(TableMetadata metadata) {
      return this.toString(metadata.comparator);
   }

   public String toString(ClusteringComparator comparator) {
      StringBuilder sb = new StringBuilder();
      sb.append(this.kind()).append('(');

      for(int i = 0; i < this.size(); ++i) {
         if(i > 0) {
            sb.append(", ");
         }

         sb.append(comparator.subtype(i).getString(this.get(i)));
      }

      return sb.append(')').toString();
   }

   public abstract ClusteringBoundOrBoundary invert();

   public static class Serializer {
      public Serializer() {
      }

      public void serialize(ClusteringBoundOrBoundary bound, DataOutputPlus out, ClusteringVersion version, List<AbstractType<?>> types) throws IOException {
         out.writeByte(bound.kind().ordinal());
         out.writeShort(bound.size());
         ClusteringPrefix.serializer.serializeValuesWithoutSize(bound, out, version, types);
      }

      public long serializedSize(ClusteringBoundOrBoundary bound, ClusteringVersion version, List<AbstractType<?>> types) {
         return (long)(1 + TypeSizes.sizeof((short)bound.size())) + ClusteringPrefix.serializer.valuesWithoutSizeSerializedSize(bound, version, types);
      }

      public ClusteringBoundOrBoundary deserialize(DataInputPlus in, ClusteringVersion version, List<AbstractType<?>> types) throws IOException {
         ClusteringPrefix.Kind kind = ClusteringPrefix.Kind.values()[in.readByte()];
         return this.deserializeValues(in, kind, version, types);
      }

      public void skipValues(DataInputPlus in, ClusteringPrefix.Kind kind, ClusteringVersion version, List<AbstractType<?>> types) throws IOException {
         int size = in.readUnsignedShort();
         if(size != 0) {
            ClusteringPrefix.serializer.skipValuesWithoutSize(in, size, version, types);
         }
      }

      public ClusteringBoundOrBoundary deserializeValues(DataInputPlus in, ClusteringPrefix.Kind kind, ClusteringVersion version, List<AbstractType<?>> types) throws IOException {
         int size = in.readUnsignedShort();
         if(size == 0) {
            return kind.isStart()?ClusteringBound.BOTTOM:ClusteringBound.TOP;
         } else {
            ByteBuffer[] values = ClusteringPrefix.serializer.deserializeValuesWithoutSize(in, size, version, types);
            return ClusteringBoundOrBoundary.create(kind, values);
         }
      }
   }
}
