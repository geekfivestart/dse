package org.apache.cassandra.serializers;

import java.nio.ByteBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;

public class LongSerializer implements TypeSerializer<Long> {
   public static final LongSerializer instance = new LongSerializer();

   public LongSerializer() {
   }

   public Long deserialize(ByteBuffer bytes) {
      return bytes.remaining() == 0?null:Long.valueOf(ByteBufferUtil.toLong(bytes));
   }

   public ByteBuffer serialize(Long value) {
      return value == null?ByteBufferUtil.EMPTY_BYTE_BUFFER:ByteBufferUtil.bytes(value.longValue());
   }

   public void validate(ByteBuffer bytes) throws MarshalException {
      if(bytes.remaining() != 8 && bytes.remaining() != 0) {
         throw new MarshalException(String.format("Expected 8 or 0 byte long (%d)", new Object[]{Integer.valueOf(bytes.remaining())}));
      }
   }

   public String toString(Long value) {
      return value == null?"":String.valueOf(value);
   }

   public Class<Long> getType() {
      return Long.class;
   }
}
