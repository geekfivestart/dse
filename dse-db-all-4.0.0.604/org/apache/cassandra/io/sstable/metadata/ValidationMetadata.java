package org.apache.cassandra.io.sstable.metadata;

import java.io.IOException;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class ValidationMetadata extends MetadataComponent {
   public static final IMetadataComponentSerializer serializer = new ValidationMetadata.ValidationMetadataSerializer();
   public final String partitioner;
   public final double bloomFilterFPChance;

   public ValidationMetadata(String partitioner, double bloomFilterFPChance) {
      this.partitioner = partitioner;
      this.bloomFilterFPChance = bloomFilterFPChance;
   }

   public MetadataType getType() {
      return MetadataType.VALIDATION;
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         ValidationMetadata that = (ValidationMetadata)o;
         return Double.compare(that.bloomFilterFPChance, this.bloomFilterFPChance) == 0 && this.partitioner.equals(that.partitioner);
      } else {
         return false;
      }
   }

   public int hashCode() {
      int result = this.partitioner.hashCode();
      long temp = Double.doubleToLongBits(this.bloomFilterFPChance);
      result = 31 * result + (int)(temp ^ temp >>> 32);
      return result;
   }

   public static class ValidationMetadataSerializer implements IMetadataComponentSerializer<ValidationMetadata> {
      public ValidationMetadataSerializer() {
      }

      public int serializedSize(Version version, ValidationMetadata component) throws IOException {
         return TypeSizes.sizeof(component.partitioner) + 8;
      }

      public void serialize(Version version, ValidationMetadata component, DataOutputPlus out) throws IOException {
         out.writeUTF(component.partitioner);
         out.writeDouble(component.bloomFilterFPChance);
      }

      public ValidationMetadata deserialize(Version version, DataInputPlus in) throws IOException {
         return new ValidationMetadata(in.readUTF(), in.readDouble());
      }
   }
}
