package org.apache.cassandra.db.marshal;

import io.netty.util.concurrent.FastThreadLocal;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Json;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.serializers.AsciiSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

public class AsciiType extends AbstractType<String> {
   public static final AsciiType instance = new AsciiType();
   private static final ArgumentDeserializer ARGUMENT_DESERIALIZER;
   private static final FastThreadLocal<CharsetEncoder> encoder;

   AsciiType() {
      super(AbstractType.ComparisonType.BYTE_ORDER, -1);
   }

   public ByteBuffer fromString(String source) {
      CharsetEncoder theEncoder = (CharsetEncoder)encoder.get();
      theEncoder.reset();

      try {
         return theEncoder.encode(CharBuffer.wrap(source));
      } catch (CharacterCodingException var4) {
         throw new MarshalException(String.format("Invalid ASCII character in string literal: %s", new Object[]{var4}));
      }
   }

   public Term fromJSONObject(Object parsed) throws MarshalException {
      try {
         return new Constants.Value(this.fromString((String)parsed));
      } catch (ClassCastException var3) {
         throw new MarshalException(String.format("Expected an ascii string, but got a %s: %s", new Object[]{parsed.getClass().getSimpleName(), parsed}));
      }
   }

   public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion) {
      try {
         return '"' + Json.quoteAsJsonString(ByteBufferUtil.string(buffer, StandardCharsets.US_ASCII)) + '"';
      } catch (CharacterCodingException var4) {
         throw new AssertionError("ascii value contained non-ascii characters: ", var4);
      }
   }

   public CQL3Type asCQL3Type() {
      return CQL3Type.Native.ASCII;
   }

   public TypeSerializer<String> getSerializer() {
      return AsciiSerializer.instance;
   }

   public ArgumentDeserializer getArgumentDeserializer() {
      return ARGUMENT_DESERIALIZER;
   }

   static {
      ARGUMENT_DESERIALIZER = new AbstractType.DefaultArgumentDerserializer(instance);
      encoder = new FastThreadLocal<CharsetEncoder>() {
         protected CharsetEncoder initialValue() {
            return StandardCharsets.US_ASCII.newEncoder();
         }
      };
   }
}
