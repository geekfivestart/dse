package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.ShortSerializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ByteSource;
import org.apache.cassandra.utils.UnsafeByteBufferAccess;
import org.apache.commons.lang3.mutable.MutableShort;

public class ShortType extends NumberType<Short> {
   public static final ShortType instance = new ShortType();

   ShortType() {
      super(AbstractType.ComparisonType.PRIMITIVE_COMPARE, -1, AbstractType.PrimitiveType.SHORT);
   }

   public static int compareType(ByteBuffer o1, ByteBuffer o2) {
      return Short.compare(UnsafeByteBufferAccess.getShort(o1), UnsafeByteBufferAccess.getShort(o2));
   }

   public ByteBuffer fromString(String source) throws MarshalException {
      if(source.isEmpty()) {
         return ByteBufferUtil.EMPTY_BYTE_BUFFER;
      } else {
         short s;
         try {
            s = Short.parseShort(source);
         } catch (Exception var4) {
            throw new MarshalException(String.format("Unable to make short from '%s'", new Object[]{source}), var4);
         }

         return this.decompose(Short.valueOf(s));
      }
   }

   public ByteSource asByteComparableSource(ByteBuffer buf) {
      return ByteSource.signedFixedLengthNumber(buf);
   }

   public Term fromJSONObject(Object parsed) throws MarshalException {
      if(!(parsed instanceof String) && !(parsed instanceof Number)) {
         throw new MarshalException(String.format("Expected a short value, but got a %s: %s", new Object[]{parsed.getClass().getSimpleName(), parsed}));
      } else {
         return new Constants.Value(this.fromString(String.valueOf(parsed)));
      }
   }

   public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion) {
      return ((Short)this.getSerializer().deserialize(buffer)).toString();
   }

   public CQL3Type asCQL3Type() {
      return CQL3Type.Native.SMALLINT;
   }

   public TypeSerializer<Short> getSerializer() {
      return ShortSerializer.instance;
   }

   public ByteBuffer add(Number left, Number right) {
      return ByteBufferUtil.bytes((short)(left.shortValue() + right.shortValue()));
   }

   public ByteBuffer substract(Number left, Number right) {
      return ByteBufferUtil.bytes((short)(left.shortValue() - right.shortValue()));
   }

   public ByteBuffer multiply(Number left, Number right) {
      return ByteBufferUtil.bytes((short)(left.shortValue() * right.shortValue()));
   }

   public ByteBuffer divide(Number left, Number right) {
      return ByteBufferUtil.bytes((short)(left.shortValue() / right.shortValue()));
   }

   public ByteBuffer mod(Number left, Number right) {
      return ByteBufferUtil.bytes((short)(left.shortValue() % right.shortValue()));
   }

   public ByteBuffer negate(Number input) {
      return ByteBufferUtil.bytes((short)(-input.shortValue()));
   }

   public ArgumentDeserializer getArgumentDeserializer() {
      return new NumberType<Short>.NumberArgumentDeserializer<MutableShort>(new MutableShort()) {
         protected void setMutableValue(MutableShort mutable, ByteBuffer buffer) {
            mutable.setValue(ByteBufferUtil.toShort(buffer));
         }
      };
   }
}
