package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.serializers.DoubleSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ByteSource;
import org.apache.cassandra.utils.UnsafeByteBufferAccess;
import org.apache.commons.lang3.mutable.MutableDouble;

public class DoubleType extends NumberType<Double> {
   public static final DoubleType instance = new DoubleType();

   DoubleType() {
      super(AbstractType.ComparisonType.PRIMITIVE_COMPARE, 8, AbstractType.PrimitiveType.DOUBLE);
   }

   public boolean isEmptyValueMeaningless() {
      return true;
   }

   public boolean isFloatingPoint() {
      return true;
   }

   public static int compareType(ByteBuffer o1, ByteBuffer o2) {
      return Double.compare(UnsafeByteBufferAccess.getDouble(o1), UnsafeByteBufferAccess.getDouble(o2));
   }

   public ByteSource asByteComparableSource(ByteBuffer buf) {
      return ByteSource.optionalSignedFixedLengthFloat(buf);
   }

   public ByteBuffer fromString(String source) throws MarshalException {
      if(source.isEmpty()) {
         return ByteBufferUtil.EMPTY_BYTE_BUFFER;
      } else {
         try {
            return this.decompose(Double.valueOf(source));
         } catch (NumberFormatException var3) {
            throw new MarshalException(String.format("Unable to make double from '%s'", new Object[]{source}), var3);
         }
      }
   }

   public Term fromJSONObject(Object parsed) throws MarshalException {
      try {
         return parsed instanceof String?new Constants.Value(this.fromString((String)parsed)):new Constants.Value(this.getSerializer().serialize(Double.valueOf(((Number)parsed).doubleValue())));
      } catch (ClassCastException var3) {
         throw new MarshalException(String.format("Expected a double value, but got a %s: %s", new Object[]{parsed.getClass().getSimpleName(), parsed}));
      }
   }

   public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion) {
      return ((Double)this.getSerializer().deserialize(buffer)).toString();
   }

   public CQL3Type asCQL3Type() {
      return CQL3Type.Native.DOUBLE;
   }

   public TypeSerializer<Double> getSerializer() {
      return DoubleSerializer.instance;
   }

   public ByteBuffer add(Number left, Number right) {
      return ByteBufferUtil.bytes(left.doubleValue() + right.doubleValue());
   }

   public ByteBuffer substract(Number left, Number right) {
      return ByteBufferUtil.bytes(left.doubleValue() - right.doubleValue());
   }

   public ByteBuffer multiply(Number left, Number right) {
      return ByteBufferUtil.bytes(left.doubleValue() * right.doubleValue());
   }

   public ByteBuffer divide(Number left, Number right) {
      return ByteBufferUtil.bytes(left.doubleValue() / right.doubleValue());
   }

   public ByteBuffer mod(Number left, Number right) {
      return ByteBufferUtil.bytes(left.doubleValue() % right.doubleValue());
   }

   public ByteBuffer negate(Number input) {
      return ByteBufferUtil.bytes(-input.doubleValue());
   }

   public ArgumentDeserializer getArgumentDeserializer() {
      return new NumberType<Double>.NumberArgumentDeserializer<MutableDouble>(new MutableDouble()) {
         protected void setMutableValue(MutableDouble mutable, ByteBuffer buffer) {
            mutable.setValue(ByteBufferUtil.toDouble(buffer));
         }
      };
   }
}
