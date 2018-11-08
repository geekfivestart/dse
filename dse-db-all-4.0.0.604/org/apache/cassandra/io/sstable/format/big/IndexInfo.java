package org.apache.cassandra.io.sstable.format.big;

import java.io.IOException;
import java.util.List;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.ClusteringVersion;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ObjectSizes;

public class IndexInfo {
   private static final long EMPTY_SIZE = ObjectSizes.measure(new IndexInfo((ClusteringPrefix)null, (ClusteringPrefix)null, 0L, 0L, (DeletionTime)null));
   public final long offset;
   public final long width;
   public final ClusteringPrefix firstName;
   public final ClusteringPrefix lastName;
   public final DeletionTime endOpenMarker;

   public IndexInfo(ClusteringPrefix firstName, ClusteringPrefix lastName, long offset, long width, DeletionTime endOpenMarker) {
      this.firstName = firstName;
      this.lastName = lastName;
      this.offset = offset;
      this.width = width;
      this.endOpenMarker = endOpenMarker;
   }

   public static IndexInfo.Serializer serializer(Version version, SerializationHeader header) {
      return new IndexInfo.Serializer(version, header.clusteringTypes());
   }

   public long unsharedHeapSize() {
      return EMPTY_SIZE + this.firstName.unsharedHeapSize() + this.lastName.unsharedHeapSize() + (this.endOpenMarker == null?0L:this.endOpenMarker.unsharedHeapSize());
   }

   public static class Serializer implements ISerializer<IndexInfo> {
      public static final long WIDTH_BASE = 65536L;
      private final ClusteringVersion version;
      private final List<AbstractType<?>> clusteringTypes;

      public Serializer(Version version, List<AbstractType<?>> clusteringTypes) {
         this.version = version.encodingVersion().clusteringVersion;
         this.clusteringTypes = clusteringTypes;
      }

      public void serialize(IndexInfo info, DataOutputPlus out) throws IOException {
         ClusteringPrefix.serializer.serialize(info.firstName, out, this.version, this.clusteringTypes);
         ClusteringPrefix.serializer.serialize(info.lastName, out, this.version, this.clusteringTypes);
         out.writeUnsignedVInt(info.offset);
         out.writeVInt(info.width - 65536L);
         out.writeBoolean(info.endOpenMarker != null);
         if(info.endOpenMarker != null) {
            DeletionTime.serializer.serialize(info.endOpenMarker, out);
         }

      }

      public void skip(DataInputPlus in) throws IOException {
         ClusteringPrefix.serializer.skip(in, this.version, this.clusteringTypes);
         ClusteringPrefix.serializer.skip(in, this.version, this.clusteringTypes);
         in.readUnsignedVInt();
         in.readVInt();
         if(in.readBoolean()) {
            DeletionTime.serializer.skip(in);
         }

      }

      public IndexInfo deserialize(DataInputPlus in) throws IOException {
         ClusteringPrefix firstName = ClusteringPrefix.serializer.deserialize(in, this.version, this.clusteringTypes);
         ClusteringPrefix lastName = ClusteringPrefix.serializer.deserialize(in, this.version, this.clusteringTypes);
         long offset = in.readUnsignedVInt();
         long width = in.readVInt() + 65536L;
         DeletionTime endOpenMarker = null;
         if(in.readBoolean()) {
            endOpenMarker = DeletionTime.serializer.deserialize(in);
         }

         return new IndexInfo(firstName, lastName, offset, width, endOpenMarker);
      }

      public long serializedSize(IndexInfo info) {
         long size = ClusteringPrefix.serializer.serializedSize(info.firstName, this.version, this.clusteringTypes) + ClusteringPrefix.serializer.serializedSize(info.lastName, this.version, this.clusteringTypes) + (long)TypeSizes.sizeofUnsignedVInt(info.offset) + (long)TypeSizes.sizeofVInt(info.width - 65536L) + (long)TypeSizes.sizeof(info.endOpenMarker != null);
         if(info.endOpenMarker != null) {
            size += DeletionTime.serializer.serializedSize(info.endOpenMarker);
         }

         return size;
      }
   }
}
