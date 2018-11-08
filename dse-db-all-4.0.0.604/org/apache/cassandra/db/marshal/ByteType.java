package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.serializers.ByteSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ByteSource;
import org.apache.commons.lang3.mutable.MutableByte;

public class ByteType extends NumberType<Byte> {
   public static final ByteType instance = new ByteType();

   ByteType() {
      super(AbstractType.ComparisonType.PRIMITIVE_COMPARE, -1, AbstractType.PrimitiveType.BYTE);
   }

   public static int compareType(ByteBuffer o1, ByteBuffer o2) {
      return o1.get(o1.position()) - o2.get(o2.position());
   }

   public ByteSource asByteComparableSource(ByteBuffer buf) {
      return ByteSource.signedFixedLengthNumber(buf);
   }

   public ByteBuffer fromString(String source) throws MarshalException {
      if(source.isEmpty()) {
         return ByteBufferUtil.EMPTY_BYTE_BUFFER;
      } else {
         byte b;
         try {
            b = Byte.parseByte(source);
         } catch (Exception var4) {
            throw new MarshalException(String.format("Unable to make byte from '%s'", new Object[]{source}), var4);
         }

         return this.decompose(Byte.valueOf(b));
      }
   }

   public Term fromJSONObject(Object parsed) throws MarshalException {
      if(!(parsed instanceof String) && !(parsed instanceof Number)) {
         throw new MarshalException(String.format("Expected a byte value, but got a %s: %s", new Object[]{parsed.getClass().getSimpleName(), parsed}));
      } else {
         return new Constants.Value(this.fromString(String.valueOf(parsed)));
      }
   }

   public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion) {
      return ((Byte)this.getSerializer().deserialize(buffer)).toString();
   }

   public CQL3Type asCQL3Type() {
      return CQL3Type.Native.TINYINT;
   }

   public TypeSerializer<Byte> getSerializer() {
      return ByteSerializer.instance;
   }

   public ByteBuffer add(Number left, Number right) {
      return ByteBufferUtil.bytes((byte)(left.byteValue() + right.byteValue()));
   }

   public ByteBuffer substract(Number left, Number right) {
      return ByteBufferUtil.bytes((byte)(left.byteValue() - right.byteValue()));
   }

   public ByteBuffer multiply(Number left, Number right) {
      return ByteBufferUtil.bytes((byte)(left.byteValue() * right.byteValue()));
   }

   public ByteBuffer divide(Number left, Number right) {
      return ByteBufferUtil.bytes((byte)(left.byteValue() / right.byteValue()));
   }

   public ByteBuffer mod(Number left, Number right) {
      return ByteBufferUtil.bytes((byte)(left.byteValue() % right.byteValue()));
   }

   public ByteBuffer negate(Number input) {
      return ByteBufferUtil.bytes((byte)(-input.byteValue()));
   }

   public ArgumentDeserializer getArgumentDeserializer() {
      return new NumberType<Byte>.NumberArgumentDeserializer<MutableByte>(new MutableByte()) {
         protected void setMutableValue(MutableByte mutable, ByteBuffer buffer) {
            mutable.setValue(ByteBufferUtil.toByte(buffer));
         }
      };
   }
}
