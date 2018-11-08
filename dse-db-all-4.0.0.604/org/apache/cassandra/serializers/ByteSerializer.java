package org.apache.cassandra.serializers;

import java.nio.ByteBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ByteSerializer implements TypeSerializer<Byte> {
   public static final ByteSerializer instance = new ByteSerializer();

   public ByteSerializer() {
   }

   public Byte deserialize(ByteBuffer bytes) {
      return bytes != null && bytes.remaining() != 0?Byte.valueOf(ByteBufferUtil.toByte(bytes)):null;
   }

   public ByteBuffer serialize(Byte value) {
      return value == null?ByteBufferUtil.EMPTY_BYTE_BUFFER:ByteBufferUtil.bytes(value.byteValue());
   }

   public void validate(ByteBuffer bytes) throws MarshalException {
      if(bytes.remaining() != 1) {
         throw new MarshalException(String.format("Expected 1 byte for a tinyint (%d)", new Object[]{Integer.valueOf(bytes.remaining())}));
      }
   }

   public String toString(Byte value) {
      return value == null?"":String.valueOf(value);
   }

   public Class<Byte> getType() {
      return Byte.class;
   }
}
