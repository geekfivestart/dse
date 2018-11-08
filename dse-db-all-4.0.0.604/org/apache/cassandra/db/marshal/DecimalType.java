package org.apache.cassandra.db.marshal;

import com.google.common.primitives.Ints;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.nio.ByteBuffer;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.serializers.DecimalSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ByteSource;

public class DecimalType extends NumberType<BigDecimal> {
   public static final DecimalType instance = new DecimalType();
   private static final ArgumentDeserializer ARGUMENT_DESERIALIZER;

   DecimalType() {
      super(AbstractType.ComparisonType.CUSTOM, -1);
   }

   public boolean isEmptyValueMeaningless() {
      return true;
   }

   public boolean isFloatingPoint() {
      return true;
   }

   public int compareCustom(ByteBuffer o1, ByteBuffer o2) {
      return ((BigDecimal)this.compose(o1)).compareTo((BigDecimal)this.compose(o2));
   }

   public ByteSource asByteComparableSource(ByteBuffer buf) {
      BigDecimal value = (BigDecimal)this.compose(buf);
      if(value.equals(BigDecimal.ZERO)) {
         return ByteSource.oneByte(128);
      } else {
         long scale = (long)value.scale() - (long)value.precision() & -2L;
         final boolean negative = value.signum() < 0;
         int negmul = negative?-1:1;
         final long exponent = -scale * (long)negmul / 2L;
         if(scale > 2147483647L || scale < -2147483648L) {
            int mv = Long.signum(scale) * 2147483647;
            value = value.scaleByPowerOfTen(mv);
            scale -= (long)mv;
         }

         final BigDecimal mantissa = value.scaleByPowerOfTen(Ints.checkedCast(scale)).stripTrailingZeros();

         assert mantissa.abs().compareTo(BigDecimal.ONE) < 0;

         return new ByteSource.WithToString() {
            int posInExp = 0;
            BigDecimal current = mantissa;

            public void reset() {
               this.posInExp = 0;
               this.current = mantissa;
            }

            public int next() {
               if(this.posInExp >= 5) {
                  if(this.current == null) {
                     return -1;
                  } else if(this.current.equals(BigDecimal.ZERO)) {
                     this.current = null;
                     return 0;
                  } else {
                     BigDecimal v = this.current.scaleByPowerOfTen(2);
                     BigDecimal floor = v.setScale(0, 3);
                     this.current = v.subtract(floor);
                     return floor.byteValueExact() + 128;
                  }
               } else if(this.posInExp != 0) {
                  return (int)(exponent >> 32 - this.posInExp++ * 8) & 255;
               } else {
                  int absexp = (int)(exponent < 0L?-exponent:exponent);

                  while(this.posInExp < 5 && absexp >> 32 - ++this.posInExp * 8 == 0) {
                     ;
                  }

                  int explen = 64 + (exponent < 0L?-1:1) * (5 - this.posInExp);
                  return explen + (negative?0:128);
               }
            }
         };
      }
   }

   public ByteBuffer fromString(String source) throws MarshalException {
      if(source.isEmpty()) {
         return ByteBufferUtil.EMPTY_BYTE_BUFFER;
      } else {
         BigDecimal decimal;
         try {
            decimal = new BigDecimal(source);
         } catch (Exception var4) {
            throw new MarshalException(String.format("unable to make BigDecimal from '%s'", new Object[]{source}), var4);
         }

         return this.decompose(decimal);
      }
   }

   public Term fromJSONObject(Object parsed) throws MarshalException {
      try {
         return new Constants.Value(this.getSerializer().serialize(new BigDecimal(parsed.toString())));
      } catch (NumberFormatException var3) {
         throw new MarshalException(String.format("Value '%s' is not a valid representation of a decimal value", new Object[]{parsed}));
      }
   }

   public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion) {
      return ((BigDecimal)this.getSerializer().deserialize(buffer)).toString();
   }

   public CQL3Type asCQL3Type() {
      return CQL3Type.Native.DECIMAL;
   }

   public TypeSerializer<BigDecimal> getSerializer() {
      return DecimalSerializer.instance;
   }

   protected BigDecimal toBigDecimal(Number number) {
      if(number instanceof BigDecimal) {
         return (BigDecimal)number;
      } else if(number instanceof BigInteger) {
         return new BigDecimal((BigInteger)number);
      } else {
         double d = number.doubleValue();
         if(Double.isNaN(d)) {
            throw new NumberFormatException("A NaN cannot be converted into a decimal");
         } else if(Double.isInfinite(d)) {
            throw new NumberFormatException("An infinite number cannot be converted into a decimal");
         } else {
            return BigDecimal.valueOf(d);
         }
      }
   }

   public ByteBuffer add(Number left, Number right) {
      return this.decompose(this.toBigDecimal(left).add(this.toBigDecimal(right), MathContext.DECIMAL128));
   }

   public ByteBuffer substract(Number left, Number right) {
      return this.decompose(this.toBigDecimal(left).subtract(this.toBigDecimal(right), MathContext.DECIMAL128));
   }

   public ByteBuffer multiply(Number left, Number right) {
      return this.decompose(this.toBigDecimal(left).multiply(this.toBigDecimal(right), MathContext.DECIMAL128));
   }

   public ByteBuffer divide(Number left, Number right) {
      return this.decompose(this.toBigDecimal(left).divide(this.toBigDecimal(right), MathContext.DECIMAL128));
   }

   public ByteBuffer mod(Number left, Number right) {
      return this.decompose(this.toBigDecimal(left).remainder(this.toBigDecimal(right), MathContext.DECIMAL128));
   }

   public ByteBuffer negate(Number input) {
      return this.decompose(this.toBigDecimal(input).negate());
   }

   public ArgumentDeserializer getArgumentDeserializer() {
      return ARGUMENT_DESERIALIZER;
   }

   static {
      ARGUMENT_DESERIALIZER = new AbstractType.DefaultArgumentDerserializer(instance);
   }
}
