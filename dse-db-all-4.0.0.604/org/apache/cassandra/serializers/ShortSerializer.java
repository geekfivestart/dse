package org.apache.cassandra.serializers;

import java.nio.ByteBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ShortSerializer implements TypeSerializer<Short> {
   public static final ShortSerializer instance = new ShortSerializer();

   public ShortSerializer() {
   }

   public Short deserialize(ByteBuffer bytes) {
      return bytes.remaining() == 0?null:Short.valueOf(ByteBufferUtil.toShort(bytes));
   }

   public ByteBuffer serialize(Short value) {
      return value == null?ByteBufferUtil.EMPTY_BYTE_BUFFER:ByteBufferUtil.bytes(value.shortValue());
   }

   public void validate(ByteBuffer bytes) throws MarshalException {
      if(bytes.remaining() != 2) {
         throw new MarshalException(String.format("Expected 2 bytes for a smallint (%d)", new Object[]{Integer.valueOf(bytes.remaining())}));
      }
   }

   public String toString(Short value) {
      return value == null?"":String.valueOf(value);
   }

   public Class<Short> getType() {
      return Short.class;
   }
}
