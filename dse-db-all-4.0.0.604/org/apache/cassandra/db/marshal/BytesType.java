package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.serializers.BytesSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Hex;

public class BytesType extends AbstractType<ByteBuffer> {
   public static final BytesType instance = new BytesType();

   BytesType() {
      super(AbstractType.ComparisonType.BYTE_ORDER, -1);
   }

   public ByteBuffer fromString(String source) {
      try {
         return ByteBuffer.wrap(Hex.hexToBytes(source));
      } catch (NumberFormatException var3) {
         throw new MarshalException(String.format("cannot parse '%s' as hex bytes", new Object[]{source}), var3);
      }
   }

   public Term fromJSONObject(Object parsed) throws MarshalException {
      try {
         String parsedString = (String)parsed;
         if(!parsedString.startsWith("0x")) {
            throw new MarshalException(String.format("String representation of blob is missing 0x prefix: %s", new Object[]{parsedString}));
         } else {
            return new Constants.Value(instance.fromString(parsedString.substring(2)));
         }
      } catch (MarshalException | ClassCastException var3) {
         throw new MarshalException(String.format("Value '%s' is not a valid blob representation: %s", new Object[]{parsed, var3.getMessage()}));
      }
   }

   public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion) {
      return "\"0x" + ByteBufferUtil.bytesToHex(buffer) + '"';
   }

   public boolean isCompatibleWith(AbstractType<?> previous) {
      return this == previous || previous == AsciiType.instance || previous == UTF8Type.instance;
   }

   public boolean isValueCompatibleWithInternal(AbstractType<?> otherType) {
      return true;
   }

   public CQL3Type asCQL3Type() {
      return CQL3Type.Native.BLOB;
   }

   public TypeSerializer<ByteBuffer> getSerializer() {
      return BytesSerializer.instance;
   }

   public ArgumentDeserializer getArgumentDeserializer() {
      return ArgumentDeserializer.NOOP_DESERIALIZER;
   }
}
