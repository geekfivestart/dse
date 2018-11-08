package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.Iterator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ByteSource;
import org.apache.cassandra.utils.FBUtilities;

public class PartitionerDefinedOrder extends AbstractType<ByteBuffer> {
   private final IPartitioner partitioner;

   public PartitionerDefinedOrder(IPartitioner partitioner) {
      super(AbstractType.ComparisonType.CUSTOM, -1);
      this.partitioner = partitioner;
   }

   public static AbstractType<?> getInstance(TypeParser parser) {
      IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
      Iterator<String> argIterator = parser.getKeyValueParameters().keySet().iterator();
      if(argIterator.hasNext()) {
         partitioner = FBUtilities.newPartitioner((String)argIterator.next());

         assert !argIterator.hasNext();
      }

      return partitioner.partitionOrdering();
   }

   public ByteBuffer compose(ByteBuffer bytes) {
      throw new UnsupportedOperationException("You can't do this with a local partitioner.");
   }

   public ByteBuffer decompose(ByteBuffer bytes) {
      throw new UnsupportedOperationException("You can't do this with a local partitioner.");
   }

   public String getString(ByteBuffer bytes) {
      return ByteBufferUtil.bytesToHex(bytes);
   }

   public ByteBuffer fromString(String source) {
      throw new UnsupportedOperationException();
   }

   public Term fromJSONObject(Object parsed) {
      throw new UnsupportedOperationException();
   }

   public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion) {
      throw new UnsupportedOperationException();
   }

   public int compareCustom(ByteBuffer o1, ByteBuffer o2) {
      return PartitionPosition.ForKey.get(o1, this.partitioner).compareTo(PartitionPosition.ForKey.get(o2, this.partitioner));
   }

   public ByteSource asByteComparableSource(ByteBuffer buf) {
      return PartitionPosition.ForKey.get(buf, this.partitioner).asByteComparableSource();
   }

   public void validate(ByteBuffer bytes) throws MarshalException {
      throw new IllegalStateException("You shouldn't be validating this.");
   }

   public TypeSerializer<ByteBuffer> getSerializer() {
      throw new UnsupportedOperationException("You can't do this with a local partitioner.");
   }

   public ArgumentDeserializer getArgumentDeserializer() {
      throw new UnsupportedOperationException("You can't do this with a local partitioner.");
   }

   public String toString() {
      return String.format("%s(%s)", new Object[]{this.getClass().getName(), this.partitioner.getClass().getName()});
   }
}
