package org.apache.cassandra.io.sstable.metadata;

import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus.Builder;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;

public class CompactionMetadata extends MetadataComponent {
   public static final IMetadataComponentSerializer serializer = new CompactionMetadata.CompactionMetadataSerializer();
   public final ICardinality cardinalityEstimator;

   public CompactionMetadata(ICardinality cardinalityEstimator) {
      this.cardinalityEstimator = cardinalityEstimator;
   }

   public MetadataType getType() {
      return MetadataType.COMPACTION;
   }

   public boolean equals(Object o) {
      return this == o?true:o != null && this.getClass() == o.getClass();
   }

   public int hashCode() {
      return 31;
   }

   public static class CompactionMetadataSerializer implements IMetadataComponentSerializer<CompactionMetadata> {
      public CompactionMetadataSerializer() {
      }

      public int serializedSize(Version version, CompactionMetadata component) throws IOException {
         int sz = 0;
         byte[] serializedCardinality = component.cardinalityEstimator.getBytes();
         return TypeSizes.sizeof(serializedCardinality.length) + serializedCardinality.length + sz;
      }

      public void serialize(Version version, CompactionMetadata component, DataOutputPlus out) throws IOException {
         ByteBufferUtil.writeWithLength((byte[])component.cardinalityEstimator.getBytes(), (DataOutput)out);
      }

      public CompactionMetadata deserialize(Version version, DataInputPlus in) throws IOException {
         ICardinality cardinality = Builder.build(ByteBufferUtil.readBytes((DataInput)in, in.readInt()));
         return new CompactionMetadata(cardinality);
      }
   }
}
