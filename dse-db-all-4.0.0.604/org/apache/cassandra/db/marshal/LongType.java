package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.serializers.LongSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ByteSource;
import org.apache.cassandra.utils.UnsafeByteBufferAccess;
import org.apache.commons.lang3.mutable.MutableLong;

public class LongType extends NumberType<Long> {
   public static final LongType instance = new LongType();

   LongType() {
      super(AbstractType.ComparisonType.PRIMITIVE_COMPARE, 8, AbstractType.PrimitiveType.LONG);
   }

   public boolean isEmptyValueMeaningless() {
      return true;
   }

   public static int compareType(ByteBuffer o1, ByteBuffer o2) {
      return compareLongs(o1, o2);
   }

   public static int compareLongs(ByteBuffer o1, ByteBuffer o2) {
      return Long.compare(UnsafeByteBufferAccess.getLong(o1), UnsafeByteBufferAccess.getLong(o2));
   }

   public ByteSource asByteComparableSource(ByteBuffer buf) {
      return ByteSource.optionalSignedFixedLengthNumber(buf);
   }

   public ByteBuffer fromString(String source) throws MarshalException {
      if(source.isEmpty()) {
         return ByteBufferUtil.EMPTY_BYTE_BUFFER;
      } else {
         long longType;
         try {
            longType = Long.parseLong(source);
         } catch (Exception var5) {
            throw new MarshalException(String.format("Unable to make long from '%s'", new Object[]{source}), var5);
         }

         return this.decompose(Long.valueOf(longType));
      }
   }

   public Term fromJSONObject(Object parsed) throws MarshalException {
      try {
         if(parsed instanceof String) {
            return new Constants.Value(this.fromString((String)parsed));
         } else {
            Number parsedNumber = (Number)parsed;
            if(!(parsedNumber instanceof Integer) && !(parsedNumber instanceof Long)) {
               throw new MarshalException(String.format("Expected a bigint value, but got a %s: %s", new Object[]{parsed.getClass().getSimpleName(), parsed}));
            } else {
               return new Constants.Value(this.getSerializer().serialize(Long.valueOf(parsedNumber.longValue())));
            }
         }
      } catch (ClassCastException var3) {
         throw new MarshalException(String.format("Expected a bigint value, but got a %s: %s", new Object[]{parsed.getClass().getSimpleName(), parsed}));
      }
   }

   public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion) {
      return ((Long)this.getSerializer().deserialize(buffer)).toString();
   }

   public boolean isValueCompatibleWithInternal(AbstractType<?> otherType) {
      return this == otherType || otherType == DateType.instance || otherType == TimestampType.instance;
   }

   public CQL3Type asCQL3Type() {
      return CQL3Type.Native.BIGINT;
   }

   public TypeSerializer<Long> getSerializer() {
      return LongSerializer.instance;
   }

   public ByteBuffer add(Number left, Number right) {
      return ByteBufferUtil.bytes(left.longValue() + right.longValue());
   }

   public ByteBuffer substract(Number left, Number right) {
      return ByteBufferUtil.bytes(left.longValue() - right.longValue());
   }

   public ByteBuffer multiply(Number left, Number right) {
      return ByteBufferUtil.bytes(left.longValue() * right.longValue());
   }

   public ByteBuffer divide(Number left, Number right) {
      return ByteBufferUtil.bytes(left.longValue() / right.longValue());
   }

   public ByteBuffer mod(Number left, Number right) {
      return ByteBufferUtil.bytes(left.longValue() % right.longValue());
   }

   public ByteBuffer negate(Number input) {
      return ByteBufferUtil.bytes(-input.longValue());
   }

   public ArgumentDeserializer getArgumentDeserializer() {
      return new NumberType<Long>.NumberArgumentDeserializer<MutableLong>(new MutableLong()) {
         protected void setMutableValue(MutableLong mutable, ByteBuffer buffer) {
            mutable.setValue(ByteBufferUtil.toLong(buffer));
         }
      };
   }
}
