package org.apache.cassandra.serializers;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;

public class DecimalSerializer implements TypeSerializer<BigDecimal> {
   static final int maxScale = Integer.getInteger("dse.decimal.maxscaleforstring", 100).intValue();
   public static final DecimalSerializer instance = new DecimalSerializer();

   public DecimalSerializer() {
   }

   public BigDecimal deserialize(ByteBuffer bytes) {
      if(bytes != null && bytes.remaining() != 0) {
         bytes = bytes.duplicate();
         int scale = bytes.getInt();
         byte[] bibytes = new byte[bytes.remaining()];
         bytes.get(bibytes);
         BigInteger bi = new BigInteger(bibytes);
         return new BigDecimal(bi, scale);
      } else {
         return null;
      }
   }

   public ByteBuffer serialize(BigDecimal value) {
      if(value == null) {
         return ByteBufferUtil.EMPTY_BYTE_BUFFER;
      } else {
         BigInteger bi = value.unscaledValue();
         int scale = value.scale();
         byte[] bibytes = bi.toByteArray();
         ByteBuffer bytes = ByteBuffer.allocate(4 + bibytes.length);
         bytes.putInt(scale);
         bytes.put(bibytes);
         bytes.rewind();
         return bytes;
      }
   }

   public void validate(ByteBuffer bytes) throws MarshalException {
      if(bytes.remaining() != 0 && bytes.remaining() < 4) {
         throw new MarshalException(String.format("Expected 0 or at least 4 bytes (%d)", new Object[]{Integer.valueOf(bytes.remaining())}));
      }
   }

   public String toString(BigDecimal value) {
      return value == null?"":(Math.abs(value.scale()) <= maxScale?value.toPlainString():value.toString());
   }

   public Class<BigDecimal> getType() {
      return BigDecimal.class;
   }
}
