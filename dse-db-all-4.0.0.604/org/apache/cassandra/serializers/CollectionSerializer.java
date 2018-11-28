package org.apache.cassandra.serializers;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class CollectionSerializer<T> implements TypeSerializer<T> {
   public CollectionSerializer() {
   }

   protected abstract List<ByteBuffer> serializeValues(T var1);

   protected abstract int getElementCount(T var1);

   public abstract T deserializeForNativeProtocol(ByteBuffer var1, ProtocolVersion var2);

   public abstract void validateForNativeProtocol(ByteBuffer var1, ProtocolVersion var2);

   public ByteBuffer serialize(T value) {
      List<ByteBuffer> values = this.serializeValues(value);
      return pack(values, this.getElementCount(value), ProtocolVersion.V3);
   }

   public T deserialize(ByteBuffer bytes) {
      return this.deserializeForNativeProtocol(bytes, ProtocolVersion.V3);
   }

   public void validate(ByteBuffer bytes) throws MarshalException {
      this.validateForNativeProtocol(bytes, ProtocolVersion.V3);
   }

   public static ByteBuffer pack(Collection<ByteBuffer> buffers, int elements, ProtocolVersion version) {
      int size = 0;
      for (ByteBuffer bb : buffers) {
         size += CollectionSerializer.sizeOfValue(bb, version);
      }
      ByteBuffer result = ByteBuffer.allocate(CollectionSerializer.sizeOfCollectionSize(elements, version) + size);
      CollectionSerializer.writeCollectionSize(result, elements, version);
      for (ByteBuffer bb : buffers) {
         CollectionSerializer.writeValue(result, bb, version);
      }
      return (ByteBuffer)result.flip();
   }

   protected static void writeCollectionSize(ByteBuffer output, int elements, ProtocolVersion version) {
      output.putInt(elements);
   }

   public static int readCollectionSize(ByteBuffer input, ProtocolVersion version) {
      return input.getInt();
   }

   protected static int sizeOfCollectionSize(int elements, ProtocolVersion version) {
      return 4;
   }

   public static void writeValue(ByteBuffer output, ByteBuffer value, ProtocolVersion version) {
      if(value == null) {
         output.putInt(-1);
      } else {
         output.putInt(value.remaining());
         output.put(value.duplicate());
      }
   }

   public static ByteBuffer readValue(ByteBuffer input, ProtocolVersion version) {
      int size = input.getInt();
      return size < 0?null:ByteBufferUtil.readBytes(input, size);
   }

   protected static void skipValue(ByteBuffer input, ProtocolVersion version) {
      int size = input.getInt();
      input.position(input.position() + size);
   }

   public static int sizeOfValue(ByteBuffer value, ProtocolVersion version) {
      return value == null?4:4 + value.remaining();
   }

   public abstract ByteBuffer getSerializedValue(ByteBuffer var1, ByteBuffer var2, AbstractType<?> var3);

   public abstract ByteBuffer getSliceFromSerialized(ByteBuffer var1, ByteBuffer var2, ByteBuffer var3, AbstractType<?> var4, boolean var5);

   protected ByteBuffer copyAsNewCollection(ByteBuffer input, int count, int startPos, int endPos, ProtocolVersion version) {
      int sizeLen = sizeOfCollectionSize(count, version);
      if(count == 0) {
         return ByteBuffer.allocate(sizeLen);
      } else {
         int bodyLen = endPos - startPos;
         ByteBuffer output = ByteBuffer.allocate(sizeLen + bodyLen);
         writeCollectionSize(output, count, version);
         output.position(0);
         ByteBufferUtil.arrayCopy(input, startPos, output, sizeLen, bodyLen);
         return output;
      }
   }
}
