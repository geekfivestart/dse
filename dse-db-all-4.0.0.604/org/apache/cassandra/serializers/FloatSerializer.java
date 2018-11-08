package org.apache.cassandra.serializers;

import java.nio.ByteBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;

public class FloatSerializer implements TypeSerializer<Float> {
   public static final FloatSerializer instance = new FloatSerializer();

   public FloatSerializer() {
   }

   public Float deserialize(ByteBuffer bytes) {
      return bytes.remaining() == 0?null:Float.valueOf(ByteBufferUtil.toFloat(bytes));
   }

   public ByteBuffer serialize(Float value) {
      return value == null?ByteBufferUtil.EMPTY_BYTE_BUFFER:ByteBufferUtil.bytes(value.floatValue());
   }

   public void validate(ByteBuffer bytes) throws MarshalException {
      if(bytes.remaining() != 4 && bytes.remaining() != 0) {
         throw new MarshalException(String.format("Expected 4 or 0 byte value for a float (%d)", new Object[]{Integer.valueOf(bytes.remaining())}));
      }
   }

   public String toString(Float value) {
      return value == null?"":String.valueOf(value);
   }

   public Class<Float> getType() {
      return Float.class;
   }
}
