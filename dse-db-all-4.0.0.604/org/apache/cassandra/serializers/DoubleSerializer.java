package org.apache.cassandra.serializers;

import java.nio.ByteBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;

public class DoubleSerializer implements TypeSerializer<Double> {
   public static final DoubleSerializer instance = new DoubleSerializer();

   public DoubleSerializer() {
   }

   public Double deserialize(ByteBuffer bytes) {
      return bytes.remaining() == 0?null:Double.valueOf(ByteBufferUtil.toDouble(bytes));
   }

   public ByteBuffer serialize(Double value) {
      return value == null?ByteBufferUtil.EMPTY_BYTE_BUFFER:ByteBufferUtil.bytes(value.doubleValue());
   }

   public void validate(ByteBuffer bytes) throws MarshalException {
      if(bytes.remaining() != 8 && bytes.remaining() != 0) {
         throw new MarshalException(String.format("Expected 8 or 0 byte value for a double (%d)", new Object[]{Integer.valueOf(bytes.remaining())}));
      }
   }

   public String toString(Double value) {
      return value == null?"":value.toString();
   }

   public Class<Double> getType() {
      return Double.class;
   }
}
