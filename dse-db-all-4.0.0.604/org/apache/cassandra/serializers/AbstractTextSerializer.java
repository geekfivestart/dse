package org.apache.cassandra.serializers;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.lang3.StringUtils;

public abstract class AbstractTextSerializer implements TypeSerializer<String> {
   private final Charset charset;

   protected AbstractTextSerializer(Charset charset) {
      this.charset = charset;
   }

   public String deserialize(ByteBuffer bytes) {
      try {
         return ByteBufferUtil.string(bytes, this.charset);
      } catch (CharacterCodingException var3) {
         throw new MarshalException("Invalid " + this.charset + " bytes " + ByteBufferUtil.bytesToHex(bytes));
      }
   }

   public ByteBuffer serialize(String value) {
      return ByteBufferUtil.bytes(value, this.charset);
   }

   public String toString(String value) {
      return value;
   }

   public Class<String> getType() {
      return String.class;
   }

   public String toCQLLiteral(ByteBuffer buffer) {
      return buffer == null?"null":'\'' + StringUtils.replace(this.deserialize(buffer), "'", "''") + '\'';
   }
}
