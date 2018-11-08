package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Json;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.UTF8Serializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

public class UTF8Type extends AbstractType<String> {
   public static final UTF8Type instance = new UTF8Type();
   private static final ArgumentDeserializer ARGUMENT_DESERIALIZER;

   UTF8Type() {
      super(AbstractType.ComparisonType.BYTE_ORDER, -1);
   }

   public ByteBuffer fromString(String source) {
      return this.decompose(source);
   }

   public Term fromJSONObject(Object parsed) throws MarshalException {
      try {
         return new Constants.Value(this.fromString((String)parsed));
      } catch (ClassCastException var3) {
         throw new MarshalException(String.format("Expected a UTF-8 string, but got a %s: %s", new Object[]{parsed.getClass().getSimpleName(), parsed}));
      }
   }

   public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion) {
      try {
         return '"' + Json.quoteAsJsonString(ByteBufferUtil.string(buffer, StandardCharsets.UTF_8)) + '"';
      } catch (CharacterCodingException var4) {
         throw new AssertionError("UTF-8 value contained non-utf8 characters: ", var4);
      }
   }

   public boolean isCompatibleWith(AbstractType<?> previous) {
      return this == previous || previous == AsciiType.instance;
   }

   public CQL3Type asCQL3Type() {
      return CQL3Type.Native.TEXT;
   }

   public TypeSerializer<String> getSerializer() {
      return UTF8Serializer.instance;
   }

   public ArgumentDeserializer getArgumentDeserializer() {
      return ARGUMENT_DESERIALIZER;
   }

   static {
      ARGUMENT_DESERIALIZER = new AbstractType.DefaultArgumentDerserializer(instance);
   }
}
