package org.apache.cassandra.serializers;

import java.nio.ByteBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;

public class BytesSerializer implements TypeSerializer<ByteBuffer> {
   public static final BytesSerializer instance = new BytesSerializer();

   public BytesSerializer() {
   }

   public ByteBuffer serialize(ByteBuffer bytes) {
      return bytes.duplicate();
   }

   public ByteBuffer deserialize(ByteBuffer value) {
      return value;
   }

   public void validate(ByteBuffer bytes) throws MarshalException {
   }

   public String toString(ByteBuffer value) {
      return ByteBufferUtil.bytesToHex(value);
   }

   public Class<ByteBuffer> getType() {
      return ByteBuffer.class;
   }

   public String toCQLLiteral(ByteBuffer buffer) {
      return buffer == null?"null":"0x" + this.toString(this.deserialize(buffer));
   }
}
