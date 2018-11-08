package org.apache.cassandra.serializers;

import java.nio.ByteBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;

public class BooleanSerializer implements TypeSerializer<Boolean> {
   private static final ByteBuffer TRUE = ByteBuffer.wrap(new byte[]{1});
   private static final ByteBuffer FALSE = ByteBuffer.wrap(new byte[]{0});
   public static final BooleanSerializer instance = new BooleanSerializer();

   public BooleanSerializer() {
   }

   public Boolean deserialize(ByteBuffer bytes) {
      if(bytes != null && bytes.remaining() != 0) {
         byte value = bytes.get(bytes.position());
         return Boolean.valueOf(value != 0);
      } else {
         return null;
      }
   }

   public ByteBuffer serialize(Boolean value) {
      return value == null?ByteBufferUtil.EMPTY_BYTE_BUFFER:(value.booleanValue()?TRUE:FALSE);
   }

   public void validate(ByteBuffer bytes) throws MarshalException {
      if(bytes.remaining() != 1 && bytes.remaining() != 0) {
         throw new MarshalException(String.format("Expected 1 or 0 byte value (%d)", new Object[]{Integer.valueOf(bytes.remaining())}));
      }
   }

   public String toString(Boolean value) {
      return value == null?"":value.toString();
   }

   public Class<Boolean> getType() {
      return Boolean.class;
   }
}
