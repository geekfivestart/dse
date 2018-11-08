package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.serializers.FloatSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ByteSource;
import org.apache.cassandra.utils.UnsafeByteBufferAccess;
import org.apache.commons.lang3.mutable.MutableFloat;

public class FloatType extends NumberType<Float> {
   public static final FloatType instance = new FloatType();

   FloatType() {
      super(AbstractType.ComparisonType.PRIMITIVE_COMPARE, 4, AbstractType.PrimitiveType.FLOAT);
   }

   public boolean isEmptyValueMeaningless() {
      return true;
   }

   public boolean isFloatingPoint() {
      return true;
   }

   public static int compareType(ByteBuffer o1, ByteBuffer o2) {
      return Float.compare(UnsafeByteBufferAccess.getFloat(o1), UnsafeByteBufferAccess.getFloat(o2));
   }

   public ByteSource asByteComparableSource(ByteBuffer buf) {
      return ByteSource.optionalSignedFixedLengthFloat(buf);
   }

   public ByteBuffer fromString(String source) throws MarshalException {
      if(source.isEmpty()) {
         return ByteBufferUtil.EMPTY_BYTE_BUFFER;
      } else {
         try {
            return this.decompose(Float.valueOf(Float.parseFloat(source)));
         } catch (NumberFormatException var3) {
            throw new MarshalException(String.format("Unable to make float from '%s'", new Object[]{source}), var3);
         }
      }
   }

   public Term fromJSONObject(Object parsed) throws MarshalException {
      try {
         return parsed instanceof String?new Constants.Value(this.fromString((String)parsed)):new Constants.Value(this.getSerializer().serialize(Float.valueOf(((Number)parsed).floatValue())));
      } catch (ClassCastException var3) {
         throw new MarshalException(String.format("Expected a float value, but got a %s: %s", new Object[]{parsed.getClass().getSimpleName(), parsed}));
      }
   }

   public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion) {
      return ((Float)this.getSerializer().deserialize(buffer)).toString();
   }

   public CQL3Type asCQL3Type() {
      return CQL3Type.Native.FLOAT;
   }

   public TypeSerializer<Float> getSerializer() {
      return FloatSerializer.instance;
   }

   public ByteBuffer add(Number left, Number right) {
      return ByteBufferUtil.bytes(left.floatValue() + right.floatValue());
   }

   public ByteBuffer substract(Number left, Number right) {
      return ByteBufferUtil.bytes(left.floatValue() - right.floatValue());
   }

   public ByteBuffer multiply(Number left, Number right) {
      return ByteBufferUtil.bytes(left.floatValue() * right.floatValue());
   }

   public ByteBuffer divide(Number left, Number right) {
      return ByteBufferUtil.bytes(left.floatValue() / right.floatValue());
   }

   public ByteBuffer mod(Number left, Number right) {
      return ByteBufferUtil.bytes(left.floatValue() % right.floatValue());
   }

   public ByteBuffer negate(Number input) {
      return ByteBufferUtil.bytes(-input.floatValue());
   }

   public ArgumentDeserializer getArgumentDeserializer() {
      return new NumberType<Float>.NumberArgumentDeserializer<MutableFloat>(new MutableFloat()) {
         protected void setMutableValue(MutableFloat mutable, ByteBuffer buffer) {
            mutable.setValue(ByteBufferUtil.toFloat(buffer));
         }
      };
   }
}
