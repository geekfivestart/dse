package org.apache.cassandra.db.marshal;

import com.google.common.primitives.UnsignedLongs;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.UUIDSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ByteSource;
import org.apache.cassandra.utils.UUIDGen;

public class UUIDType extends AbstractType<UUID> {
   public static final UUIDType instance = new UUIDType();
   private static final ArgumentDeserializer ARGUMENT_DESERIALIZER;
   static final Pattern regexPattern;

   UUIDType() {
      super(AbstractType.ComparisonType.CUSTOM, 16);
   }

   public boolean isEmptyValueMeaningless() {
      return true;
   }

   public int compareCustom(ByteBuffer b1, ByteBuffer b2) {
      int s1 = b1.position();
      int s2 = b2.position();
      int l1 = b1.limit();
      int l2 = b2.limit();
      boolean p1 = l1 - s1 == 16;
      boolean p2 = l2 - s2 == 16;
      if(!(p1 & p2)) {
         assert p1 | l1 == s1;

         assert p2 | l2 == s2;

         return p1?1:(p2?-1:0);
      } else {
         long msb1 = b1.getLong(s1);
         long msb2 = b2.getLong(s2);
         int version1 = (int)(msb1 >>> 12 & 15L);
         int version2 = (int)(msb2 >>> 12 & 15L);
         if(version1 != version2) {
            return version1 - version2;
         } else {
            if(version1 == 1) {
               long reorder1 = TimeUUIDType.reorderTimestampBytes(msb1);
               long reorder2 = TimeUUIDType.reorderTimestampBytes(msb2);
               int c = Long.compare(reorder1, reorder2);
               if(c != 0) {
                  return c;
               }
            } else {
               int c = UnsignedLongs.compare(msb1, msb2);
               if(c != 0) {
                  return c;
               }
            }

            return UnsignedLongs.compare(b1.getLong(s1 + 8), b2.getLong(s2 + 8));
         }
      }
   }

   public ByteSource asByteComparableSource(ByteBuffer b) {
      if(!b.hasRemaining()) {
         return null;
      } else {
         int s = b.position();
         long msb = b.getLong(s);
         long version = msb >>> 12 & 15L;
         ByteBuffer swizzled = ByteBuffer.allocate(16);
         if(version == 1L) {
            swizzled.putLong(0, TimeUUIDType.reorderTimestampBytes(msb));
         } else {
            swizzled.putLong(0, version << 60 | msb >>> 4 & 1152921504606842880L | msb & 4095L);
         }

         swizzled.putLong(8, b.getLong(s + 8));
         return ByteSource.fixedLength(swizzled);
      }
   }

   public boolean isValueCompatibleWithInternal(AbstractType<?> otherType) {
      return otherType instanceof UUIDType || otherType instanceof TimeUUIDType;
   }

   public ByteBuffer fromString(String source) throws MarshalException {
      ByteBuffer parsed = parse(source);
      if(parsed != null) {
         return parsed;
      } else {
         throw new MarshalException(String.format("Unable to make UUID from '%s'", new Object[]{source}));
      }
   }

   public CQL3Type asCQL3Type() {
      return CQL3Type.Native.UUID;
   }

   public TypeSerializer<UUID> getSerializer() {
      return UUIDSerializer.instance;
   }

   static ByteBuffer parse(String source) {
      if(source.isEmpty()) {
         return ByteBufferUtil.EMPTY_BYTE_BUFFER;
      } else if(regexPattern.matcher(source).matches()) {
         try {
            return UUIDGen.toByteBuffer(UUID.fromString(source));
         } catch (IllegalArgumentException var2) {
            throw new MarshalException(String.format("Unable to make UUID from '%s'", new Object[]{source}), var2);
         }
      } else {
         return null;
      }
   }

   public Term fromJSONObject(Object parsed) throws MarshalException {
      try {
         return new Constants.Value(this.fromString((String)parsed));
      } catch (ClassCastException var3) {
         throw new MarshalException(String.format("Expected a string representation of a uuid, but got a %s: %s", new Object[]{parsed.getClass().getSimpleName(), parsed}));
      }
   }

   static int version(ByteBuffer uuid) {
      return (uuid.get(6) & 240) >> 4;
   }

   public ArgumentDeserializer getArgumentDeserializer() {
      return ARGUMENT_DESERIALIZER;
   }

   static {
      ARGUMENT_DESERIALIZER = new AbstractType.DefaultArgumentDerserializer(instance);
      regexPattern = Pattern.compile("[A-Fa-f0-9]{8}\\-[A-Fa-f0-9]{4}\\-[A-Fa-f0-9]{4}\\-[A-Fa-f0-9]{4}\\-[A-Fa-f0-9]{12}");
   }
}
