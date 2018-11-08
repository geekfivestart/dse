package org.apache.cassandra.db.marshal;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.serializers.IntegerSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ByteSource;

public final class IntegerType extends NumberType<BigInteger> {
   public static final IntegerType instance = new IntegerType();
   private static final ArgumentDeserializer ARGUMENT_DESERIALIZER;

   private static int findMostSignificantByte(ByteBuffer bytes) {
      int len = bytes.remaining() - 1;

      int i;
      for(i = 0; i < len; ++i) {
         byte b0 = bytes.get(bytes.position() + i);
         if(b0 != 0 && b0 != -1) {
            break;
         }

         byte b1 = bytes.get(bytes.position() + i + 1);
         if(b0 == 0 && b1 != 0) {
            if(b1 > 0) {
               ++i;
            }
            break;
         }

         if(b0 == -1 && b1 != -1) {
            if(b1 < 0) {
               ++i;
            }
            break;
         }
      }

      return i;
   }

   IntegerType() {
      super(AbstractType.ComparisonType.CUSTOM, -1);
   }

   public boolean isEmptyValueMeaningless() {
      return true;
   }

   public int compareCustom(ByteBuffer lhs, ByteBuffer rhs) {
      return compareIntegers(lhs, rhs);
   }

   public static int compareIntegers(ByteBuffer lhs, ByteBuffer rhs) {
      int lhsLen = lhs.remaining();
      int rhsLen = rhs.remaining();
      if(lhsLen == 0) {
         return rhsLen == 0?0:-1;
      } else if(rhsLen == 0) {
         return 1;
      } else {
         int lhsMsbIdx = findMostSignificantByte(lhs);
         int rhsMsbIdx = findMostSignificantByte(rhs);
         int lhsLenDiff = lhsLen - lhsMsbIdx;
         int rhsLenDiff = rhsLen - rhsMsbIdx;
         byte lhsMsb = lhs.get(lhs.position() + lhsMsbIdx);
         byte rhsMsb = rhs.get(rhs.position() + rhsMsbIdx);
         if(lhsLenDiff != rhsLenDiff) {
            return lhsMsb < 0?(rhsMsb < 0?rhsLenDiff - lhsLenDiff:-1):(rhsMsb < 0?1:lhsLenDiff - rhsLenDiff);
         } else if(lhsMsb != rhsMsb) {
            return lhsMsb - rhsMsb;
         } else {
            ++lhsMsbIdx;
            ++rhsMsbIdx;

            do {
               if(lhsMsbIdx >= lhsLen) {
                  return 0;
               }

               lhsMsb = lhs.get(lhs.position() + lhsMsbIdx++);
               rhsMsb = rhs.get(rhs.position() + rhsMsbIdx++);
            } while(lhsMsb == rhsMsb);

            return (lhsMsb & 255) - (rhsMsb & 255);
         }
      }
   }

   public ByteSource asByteComparableSource(final ByteBuffer buf) {
      final int p = buf.position();
      final int limit = buf.limit();
      if(p == limit) {
         return null;
      } else {
         final byte signbyte = buf.get(p);
         if(signbyte == 0 || signbyte == -1) {
            while(p + 1 < limit) {
               ++p;
               if(buf.get(p) != signbyte) {
                  break;
               }
            }
         }

         return new ByteSource.WithToString() {
            int pos = p;
            int sizeToReport = limit - p;
            boolean sizeReported = false;

            public void reset() {
               this.pos = p;
               this.sizeToReport = limit - p;
               this.sizeReported = false;
            }

            public int next() {
               if(!this.sizeReported) {
                  int v = this.sizeToReport;
                  if(v >= 128) {
                     v = 128;
                  } else {
                     this.sizeReported = true;
                  }

                  this.sizeToReport -= v;
                  return signbyte >= 0?128 + (v - 1):127 - (v - 1);
               } else {
                  return this.pos == limit?-1:buf.get(this.pos++) & 255;
               }
            }
         };
      }
   }

   public ByteBuffer fromString(String source) throws MarshalException {
      if(source.isEmpty()) {
         return ByteBufferUtil.EMPTY_BYTE_BUFFER;
      } else {
         BigInteger integerType;
         try {
            integerType = new BigInteger(source);
         } catch (Exception var4) {
            throw new MarshalException(String.format("unable to make int from '%s'", new Object[]{source}), var4);
         }

         return this.decompose(integerType);
      }
   }

   public Term fromJSONObject(Object parsed) throws MarshalException {
      try {
         return new Constants.Value(this.getSerializer().serialize(new BigInteger(parsed.toString())));
      } catch (NumberFormatException var3) {
         throw new MarshalException(String.format("Value '%s' is not a valid representation of a varint value", new Object[]{parsed}));
      }
   }

   public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion) {
      return ((BigInteger)this.getSerializer().deserialize(buffer)).toString();
   }

   public boolean isValueCompatibleWithInternal(AbstractType<?> otherType) {
      return this == otherType || Int32Type.instance.isValueCompatibleWith(otherType) || LongType.instance.isValueCompatibleWith(otherType);
   }

   public CQL3Type asCQL3Type() {
      return CQL3Type.Native.VARINT;
   }

   public TypeSerializer<BigInteger> getSerializer() {
      return IntegerSerializer.instance;
   }

   private BigInteger toBigInteger(Number number) {
      return number instanceof BigInteger?(BigInteger)number:BigInteger.valueOf(number.longValue());
   }

   public ByteBuffer add(Number left, Number right) {
      return this.decompose(this.toBigInteger(left).add(this.toBigInteger(right)));
   }

   public ByteBuffer substract(Number left, Number right) {
      return this.decompose(this.toBigInteger(left).subtract(this.toBigInteger(right)));
   }

   public ByteBuffer multiply(Number left, Number right) {
      return this.decompose(this.toBigInteger(left).multiply(this.toBigInteger(right)));
   }

   public ByteBuffer divide(Number left, Number right) {
      return this.decompose(this.toBigInteger(left).divide(this.toBigInteger(right)));
   }

   public ByteBuffer mod(Number left, Number right) {
      return this.decompose(this.toBigInteger(left).remainder(this.toBigInteger(right)));
   }

   public ByteBuffer negate(Number input) {
      return this.decompose(this.toBigInteger(input).negate());
   }

   public ArgumentDeserializer getArgumentDeserializer() {
      return ARGUMENT_DESERIALIZER;
   }

   static {
      ARGUMENT_DESERIALIZER = new AbstractType.DefaultArgumentDerserializer(instance);
   }
}
