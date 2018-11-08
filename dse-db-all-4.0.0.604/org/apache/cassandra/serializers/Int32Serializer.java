package org.apache.cassandra.serializers;

import java.nio.ByteBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;

public class Int32Serializer implements TypeSerializer<Integer> {
   public static final Int32Serializer instance = new Int32Serializer();

   public Int32Serializer() {
   }

   public Integer deserialize(ByteBuffer bytes) {
      return bytes.remaining() == 0?null:Integer.valueOf(ByteBufferUtil.toInt(bytes));
   }

   public ByteBuffer serialize(Integer value) {
      return value == null?ByteBufferUtil.EMPTY_BYTE_BUFFER:ByteBufferUtil.bytes(value.intValue());
   }

   public void validate(ByteBuffer bytes) throws MarshalException {
      if(bytes.remaining() != 4 && bytes.remaining() != 0) {
         throw new MarshalException(String.format("Expected 4 or 0 byte int (%d)", new Object[]{Integer.valueOf(bytes.remaining())}));
      }
   }

   public String toString(Integer value) {
      return value == null?"":String.valueOf(value);
   }

   public Class<Integer> getType() {
      return Integer.class;
   }
}
