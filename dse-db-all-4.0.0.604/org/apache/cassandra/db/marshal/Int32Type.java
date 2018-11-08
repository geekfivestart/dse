package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.serializers.Int32Serializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ByteSource;
import org.apache.cassandra.utils.UnsafeByteBufferAccess;
import org.apache.commons.lang3.mutable.MutableInt;

public class Int32Type extends NumberType<Integer> {
   public static final Int32Type instance = new Int32Type();

   Int32Type() {
      super(AbstractType.ComparisonType.PRIMITIVE_COMPARE, 4, AbstractType.PrimitiveType.INT32);
   }

   public boolean isEmptyValueMeaningless() {
      return true;
   }

   public static int compareType(ByteBuffer o1, ByteBuffer o2) {
      return Integer.compare(UnsafeByteBufferAccess.getInt(o1), UnsafeByteBufferAccess.getInt(o2));
   }

   public ByteSource asByteComparableSource(ByteBuffer buf) {
      return ByteSource.optionalSignedFixedLengthNumber(buf);
   }

   public ByteBuffer fromString(String source) throws MarshalException {
      if(source.isEmpty()) {
         return ByteBufferUtil.EMPTY_BYTE_BUFFER;
      } else {
         int int32Type;
         try {
            int32Type = Integer.parseInt(source);
         } catch (Exception var4) {
            throw new MarshalException(String.format("Unable to make int from '%s'", new Object[]{source}), var4);
         }

         return this.decompose(Integer.valueOf(int32Type));
      }
   }

   public Term fromJSONObject(Object parsed) throws MarshalException {
      try {
         if(parsed instanceof String) {
            return new Constants.Value(this.fromString((String)parsed));
         } else {
            Number parsedNumber = (Number)parsed;
            if(!(parsedNumber instanceof Integer)) {
               throw new MarshalException(String.format("Expected an int value, but got a %s: %s", new Object[]{parsed.getClass().getSimpleName(), parsed}));
            } else {
               return new Constants.Value(this.getSerializer().serialize(Integer.valueOf(parsedNumber.intValue())));
            }
         }
      } catch (ClassCastException var3) {
         throw new MarshalException(String.format("Expected an int value, but got a %s: %s", new Object[]{parsed.getClass().getSimpleName(), parsed}));
      }
   }

   public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion) {
      return ((Integer)this.getSerializer().deserialize(buffer)).toString();
   }

   public CQL3Type asCQL3Type() {
      return CQL3Type.Native.INT;
   }

   public TypeSerializer<Integer> getSerializer() {
      return Int32Serializer.instance;
   }

   public ByteBuffer add(Number left, Number right) {
      return ByteBufferUtil.bytes(left.intValue() + right.intValue());
   }

   public ByteBuffer substract(Number left, Number right) {
      return ByteBufferUtil.bytes(left.intValue() - right.intValue());
   }

   public ByteBuffer multiply(Number left, Number right) {
      return ByteBufferUtil.bytes(left.intValue() * right.intValue());
   }

   public ByteBuffer divide(Number left, Number right) {
      return ByteBufferUtil.bytes(left.intValue() / right.intValue());
   }

   public ByteBuffer mod(Number left, Number right) {
      return ByteBufferUtil.bytes(left.intValue() % right.intValue());
   }

   public ByteBuffer negate(Number input) {
      return ByteBufferUtil.bytes(-input.intValue());
   }

   public ArgumentDeserializer getArgumentDeserializer() {
      return new NumberType<Integer>.NumberArgumentDeserializer<MutableInt>(new MutableInt()) {
         protected void setMutableValue(MutableInt mutable, ByteBuffer buffer) {
            mutable.setValue(ByteBufferUtil.toInt(buffer));
         }
      };
   }
}
