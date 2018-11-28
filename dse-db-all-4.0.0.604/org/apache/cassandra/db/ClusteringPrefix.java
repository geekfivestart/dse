package org.apache.cassandra.db;

import com.google.common.hash.Hasher;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

public interface ClusteringPrefix extends IMeasurableMemory, Clusterable {
   ClusteringPrefix.Serializer serializer = new ClusteringPrefix.Serializer();

   ClusteringPrefix.Kind kind();

   int size();

   ByteBuffer get(int var1);

   void digest(Hasher var1);

   int dataSize();

   String toString(TableMetadata var1);

   default ByteBuffer serializeAsPartitionKey() {
      if(this.size() == 1) {
         return this.get(0);
      } else {
         ByteBuffer[] values = new ByteBuffer[this.size()];

         for(int i = 0; i < this.size(); ++i) {
            values[i] = this.get(i);
         }

         return CompositeType.build(values);
      }
   }

   default String clusteringString(List<AbstractType<?>> types) {
      StringBuilder sb = new StringBuilder();
      sb.append(this.kind()).append('(');

      for(int i = 0; i < this.size(); ++i) {
         if(i > 0) {
            sb.append(", ");
         }

         sb.append(((AbstractType)types.get(i)).getString(this.get(i)));
      }

      return sb.append(')').toString();
   }

   ByteBuffer[] getRawValues();

   public static class Deserializer {
      private final ClusteringComparator comparator;
      private final DataInputPlus in;
      private final SerializationHeader serializationHeader;
      private boolean nextIsRow;
      private long nextHeader;
      private int nextSize;
      private ClusteringPrefix.Kind nextKind;
      private int deserializedSize;
      private ByteBuffer[] nextValues;

      public Deserializer(ClusteringComparator comparator, DataInputPlus in, SerializationHeader header) {
         this.comparator = comparator;
         this.in = in;
         this.serializationHeader = header;
      }

      public void prepare(int flags, int extendedFlags) throws IOException {
         if(UnfilteredSerializer.isStatic(extendedFlags)) {
            throw new IOException("Corrupt flags value for clustering prefix (isStatic flag set): " + flags);
         } else {
            this.nextIsRow = UnfilteredSerializer.kind(flags) == Unfiltered.Kind.ROW;
            this.nextKind = this.nextIsRow?ClusteringPrefix.Kind.CLUSTERING:ClusteringPrefix.Kind.values()[this.in.readByte()];
            this.nextSize = this.nextIsRow?this.comparator.size():this.in.readUnsignedShort();
            this.deserializedSize = 0;
            if(this.nextValues == null) {
               this.nextValues = new ByteBuffer[this.nextSize];
            } else if(this.nextValues.length < this.nextSize) {
               this.nextValues = (ByteBuffer[])Arrays.copyOf(this.nextValues, this.nextSize);
            }

         }
      }

      public int compareNextTo(ClusteringBoundOrBoundary bound) throws IOException {
         if(bound == ClusteringBound.TOP) {
            return -1;
         } else {
            for(int i = 0; i < bound.size(); ++i) {
               if(!this.hasComponent(i)) {
                  return this.nextKind.comparedToClustering;
               }

               int cmp = this.comparator.compareComponent(i, this.nextValues[i], bound.get(i));
               if(cmp != 0) {
                  return cmp;
               }
            }

            if(bound.size() == this.nextSize) {
               return ClusteringPrefix.Kind.compare(this.nextKind, bound.kind());
            } else {
               return -bound.kind().comparedToClustering;
            }
         }
      }

      private boolean hasComponent(int i) throws IOException {
         if(i >= this.nextSize) {
            return false;
         } else {
            while(this.deserializedSize <= i) {
               this.deserializeOne();
            }

            return true;
         }
      }

      private boolean deserializeOne() throws IOException {
         if(this.deserializedSize == this.nextSize) {
            return false;
         } else {
            if(this.deserializedSize % 32 == 0) {
               this.nextHeader = this.in.readUnsignedVInt();
            }

            int i = this.deserializedSize++;
            ByteBuffer nextValue;
            if(ClusteringPrefix.Serializer.isNull(this.nextHeader, i)) {
               nextValue = null;
            } else if(ClusteringPrefix.Serializer.isEmpty(this.nextHeader, i)) {
               nextValue = ByteBufferUtil.EMPTY_BYTE_BUFFER;
            } else {
               AbstractType<?> type = (AbstractType)this.serializationHeader.clusteringTypes().get(i);
               nextValue = type.readValue(this.in, DatabaseDescriptor.getMaxValueSize(), this.nextValues[i]);
            }

            this.nextValues[i] = nextValue;
            return true;
         }
      }

      private void deserializeAll() throws IOException {
         while(this.deserializeOne()) {
            ;
         }

      }

      public ClusteringBoundOrBoundary deserializeNextBound() throws IOException {
         assert !this.nextIsRow;

         this.deserializeAll();
         ByteBuffer[] values = this.detachValues();
         ClusteringBoundOrBoundary bound = ClusteringBoundOrBoundary.create(this.nextKind, values);
         return bound;
      }

      public Clustering deserializeNextClustering() throws IOException {
         assert this.nextIsRow;

         this.deserializeAll();
         ByteBuffer[] values = this.detachValues();
         Clustering clustering = Clustering.make(values);
         return clustering;
      }

      private ByteBuffer[] detachValues() {
         if(this.nextSize == 0) {
            return ByteBufferUtil.EMPTY_BUFFER_ARRAY;
         } else {
            ByteBuffer[] values = (ByteBuffer[])Arrays.copyOf(this.nextValues, this.nextSize);
            Arrays.fill(this.nextValues, 0, this.nextSize, (Object)null);
            return values;
         }
      }

      public ClusteringPrefix.Kind skipNext() throws IOException {
         for(int i = this.deserializedSize; i < this.nextSize; ++i) {
            if(i % 32 == 0) {
               this.nextHeader = this.in.readUnsignedVInt();
            }

            if(!ClusteringPrefix.Serializer.isNull(this.nextHeader, i) && !ClusteringPrefix.Serializer.isEmpty(this.nextHeader, i)) {
               ((AbstractType)this.serializationHeader.clusteringTypes().get(i)).skipValue(this.in);
            }
         }

         this.deserializedSize = this.nextSize;
         return this.nextKind;
      }
   }

   public static class Serializer {
      public Serializer() {
      }

      public void serialize(ClusteringPrefix clustering, DataOutputPlus out, ClusteringVersion version, List<AbstractType<?>> types) throws IOException {
         assert clustering.kind() != ClusteringPrefix.Kind.STATIC_CLUSTERING;

         if(clustering.kind() == ClusteringPrefix.Kind.CLUSTERING) {
            out.writeByte(clustering.kind().ordinal());
            Clustering.serializer.serialize((Clustering)clustering, out, version, types);
         } else {
            ClusteringBoundOrBoundary.serializer.serialize((ClusteringBoundOrBoundary)clustering, out, version, types);
         }

      }

      public void skip(DataInputPlus in, ClusteringVersion version, List<AbstractType<?>> types) throws IOException {
         ClusteringPrefix.Kind kind = ClusteringPrefix.Kind.values()[in.readByte()];

         assert kind != ClusteringPrefix.Kind.STATIC_CLUSTERING;

         if(kind == ClusteringPrefix.Kind.CLUSTERING) {
            Clustering.serializer.skip(in, version, types);
         } else {
            ClusteringBoundOrBoundary.serializer.skipValues(in, kind, version, types);
         }

      }

      public ClusteringPrefix deserialize(DataInputPlus in, ClusteringVersion version, List<AbstractType<?>> types) throws IOException {
         ClusteringPrefix.Kind kind = ClusteringPrefix.Kind.values()[in.readByte()];

         assert kind != ClusteringPrefix.Kind.STATIC_CLUSTERING;

         return (ClusteringPrefix)(kind == ClusteringPrefix.Kind.CLUSTERING?Clustering.serializer.deserialize(in, version, types):ClusteringBoundOrBoundary.serializer.deserializeValues(in, kind, version, types));
      }

      public long serializedSize(ClusteringPrefix clustering, ClusteringVersion version, List<AbstractType<?>> types) {
         assert clustering.kind() != ClusteringPrefix.Kind.STATIC_CLUSTERING;

         return clustering.kind() == ClusteringPrefix.Kind.CLUSTERING?1L + Clustering.serializer.serializedSize((Clustering)clustering, version, types):ClusteringBoundOrBoundary.serializer.serializedSize((ClusteringBoundOrBoundary)clustering, version, types);
      }

      void serializeValuesWithoutSize(ClusteringPrefix clustering, DataOutputPlus out, ClusteringVersion version, List<AbstractType<?>> types) throws IOException {
         int offset = 0;
         int clusteringSize = clustering.size();

         while(offset < clusteringSize) {
            int limit = Math.min(clusteringSize, offset + 32);
            out.writeUnsignedVInt(makeHeader(clustering, offset, limit));

            for(; offset < limit; ++offset) {
               ByteBuffer v = clustering.get(offset);
               if(v != null && v.hasRemaining()) {
                  ((AbstractType)types.get(offset)).writeValue(v, out);
               }
            }
         }

      }

      long valuesWithoutSizeSerializedSize(ClusteringPrefix clustering, ClusteringVersion version, List<AbstractType<?>> types) {
         long result = 0L;
         int offset = 0;

         int clusteringSize;
         int i;
         for(clusteringSize = clustering.size(); offset < clusteringSize; offset = i) {
            i = Math.min(clusteringSize, offset + 32);
            result += (long)TypeSizes.sizeofUnsignedVInt(makeHeader(clustering, offset, i));
         }

         for(i = 0; i < clusteringSize; ++i) {
            ByteBuffer v = clustering.get(i);
            if(v != null && v.hasRemaining()) {
               result += (long)((AbstractType)types.get(i)).writtenLength(v);
            }
         }

         return result;
      }

      ByteBuffer[] deserializeValuesWithoutSize(DataInputPlus in, int size, ClusteringVersion version, List<AbstractType<?>> types) throws IOException {
         assert size > 0;

         ByteBuffer[] values = new ByteBuffer[size];
         int offset = 0;

         while(offset < size) {
            long header = in.readUnsignedVInt();

            for(int limit = Math.min(size, offset + 32); offset < limit; ++offset) {
               values[offset] = isNull(header, offset)?null:(isEmpty(header, offset)?ByteBufferUtil.EMPTY_BYTE_BUFFER:((AbstractType)types.get(offset)).readValue(in, DatabaseDescriptor.getMaxValueSize()));
            }
         }

         return values;
      }

      void skipValuesWithoutSize(DataInputPlus in, int size, ClusteringVersion version, List<AbstractType<?>> types) throws IOException {
         assert size > 0;

         int offset = 0;

         while(offset < size) {
            long header = in.readUnsignedVInt();

            for(int limit = Math.min(size, offset + 32); offset < limit; ++offset) {
               if(!isNull(header, offset) && !isEmpty(header, offset)) {
                  ((AbstractType)types.get(offset)).skipValue(in);
               }
            }
         }

      }

      private static long makeHeader(ClusteringPrefix clustering, int offset, int limit) {
         long header = 0L;

         for(int i = offset; i < limit; ++i) {
            ByteBuffer v = clustering.get(i);
            if(v == null) {
               header |= 1L << i * 2 + 1;
            } else if(!v.hasRemaining()) {
               header |= 1L << i * 2;
            }
         }

         return header;
      }

      private static boolean isNull(long header, int i) {
         long mask = 1L << i * 2 + 1;
         return (header & mask) != 0L;
      }

      private static boolean isEmpty(long header, int i) {
         long mask = 1L << i * 2;
         return (header & mask) != 0L;
      }
   }

   public static enum Kind {
      EXCL_END_BOUND(0, -1, 32),
      INCL_START_BOUND(0, -1, 32),
      EXCL_END_INCL_START_BOUNDARY(0, -1, 32),
      STATIC_CLUSTERING(1, -1, 33),
      CLUSTERING(2, 0, 64),
      INCL_END_EXCL_START_BOUNDARY(3, 1, 96),
      INCL_END_BOUND(3, 1, 96),
      EXCL_START_BOUND(3, 1, 96);

      private final int comparison;
      public final int comparedToClustering;
      public final int byteSourceValue;

      private Kind(int comparison, int comparedToClustering, int byteSourceValue) {
         this.comparison = comparison;
         this.comparedToClustering = comparedToClustering;
         this.byteSourceValue = byteSourceValue;
      }

      public static int compare(ClusteringPrefix.Kind k1, ClusteringPrefix.Kind k2) {
         return Integer.compare(k1.comparison, k2.comparison);
      }

      public Kind invert() {
         switch (this) {
            case EXCL_START_BOUND: {
               return INCL_END_BOUND;
            }
            case INCL_START_BOUND: {
               return EXCL_END_BOUND;
            }
            case EXCL_END_BOUND: {
               return INCL_START_BOUND;
            }
            case INCL_END_BOUND: {
               return EXCL_START_BOUND;
            }
            case EXCL_END_INCL_START_BOUNDARY: {
               return INCL_END_EXCL_START_BOUNDARY;
            }
            case INCL_END_EXCL_START_BOUNDARY: {
               return EXCL_END_INCL_START_BOUNDARY;
            }
         }
         return this;
      }

      public boolean isBound() {
         switch (this) {
            case EXCL_START_BOUND:
            case INCL_START_BOUND:
            case EXCL_END_BOUND:
            case INCL_END_BOUND: {
               return true;
            }
         }
         return false;
      }

      public boolean isBoundary() {
         switch (this) {
            case EXCL_END_INCL_START_BOUNDARY:
            case INCL_END_EXCL_START_BOUNDARY: {
               return true;
            }
         }
         return false;
      }

      public boolean isStart() {
         switch (this) {
            case EXCL_START_BOUND:
            case INCL_START_BOUND:
            case EXCL_END_INCL_START_BOUNDARY:
            case INCL_END_EXCL_START_BOUNDARY: {
               return true;
            }
         }
         return false;
      }

      public boolean isEnd() {
         switch (this) {
            case EXCL_END_BOUND:
            case INCL_END_BOUND:
            case EXCL_END_INCL_START_BOUNDARY:
            case INCL_END_EXCL_START_BOUNDARY: {
               return true;
            }
         }
         return false;
      }

      public boolean isOpen(boolean reversed) {
         return this.isBoundary() || (reversed ? this.isEnd() : this.isStart());
      }

      public boolean isClose(boolean reversed) {
         return this.isBoundary() || (reversed ? this.isStart() : this.isEnd());
      }

      public ClusteringPrefix.Kind closeBoundOfBoundary(boolean reversed) {
         assert this.isBoundary();

         return reversed?(this == INCL_END_EXCL_START_BOUNDARY?EXCL_START_BOUND:INCL_START_BOUND):(this == INCL_END_EXCL_START_BOUNDARY?INCL_END_BOUND:EXCL_END_BOUND);
      }

      public ClusteringPrefix.Kind openBoundOfBoundary(boolean reversed) {
         assert this.isBoundary();

         return reversed?(this == INCL_END_EXCL_START_BOUNDARY?INCL_END_BOUND:EXCL_END_BOUND):(this == INCL_END_EXCL_START_BOUNDARY?EXCL_START_BOUND:INCL_START_BOUND);
      }

      public int asByteComparableValue() {
         return this.byteSourceValue;
      }
   }
}
