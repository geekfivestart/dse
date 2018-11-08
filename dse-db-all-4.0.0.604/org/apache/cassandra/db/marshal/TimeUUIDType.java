package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Duration;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TimeUUIDSerializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.utils.ByteSource;
import org.apache.cassandra.utils.UUIDGen;

public class TimeUUIDType extends TemporalType<UUID> {
   public static final TimeUUIDType instance = new TimeUUIDType();

   TimeUUIDType() {
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
         msb1 = reorderTimestampBytes(msb1);
         msb2 = reorderTimestampBytes(msb2);

         assert (msb1 & topbyte(240L)) == topbyte(16L);

         assert (msb2 & topbyte(240L)) == topbyte(16L);

         int c = Long.compare(msb1, msb2);
         if(c != 0) {
            return c;
         } else {
            long lsb1 = signedBytesToNativeLong(b1.getLong(s1 + 8));
            long lsb2 = signedBytesToNativeLong(b2.getLong(s2 + 8));
            return Long.compare(lsb1, lsb2);
         }
      }
   }

   public ByteSource asByteComparableSource(ByteBuffer b) {
      if(!b.hasRemaining()) {
         return null;
      } else {
         int s = b.position();
         long msb = b.getLong(s);

         assert (msb >>> 12 & 15L) == 1L;

         ByteBuffer swizzled = ByteBuffer.allocate(16);
         swizzled.putLong(0, reorderTimestampBytes(msb));
         swizzled.putLong(8, b.getLong(s + 8) ^ -9187201950435737472L);
         return ByteSource.fixedLength(swizzled);
      }
   }

   private static long signedBytesToNativeLong(long signedBytes) {
      return signedBytes ^ 36170086419038336L;
   }

   private static long topbyte(long topbyte) {
      return topbyte << 56;
   }

   protected static long reorderTimestampBytes(long input) {
      return input << 48 | input << 16 & 281470681743360L | input >>> 32;
   }

   public ByteBuffer fromString(String source) throws MarshalException {
      ByteBuffer parsed = UUIDType.parse(source);
      if(parsed == null) {
         throw new MarshalException(String.format("Unknown timeuuid representation: %s", new Object[]{source}));
      } else if(parsed.remaining() == 16 && UUIDType.version(parsed) != 1) {
         throw new MarshalException("TimeUUID supports only version 1 UUIDs");
      } else {
         return parsed;
      }
   }

   public Term fromJSONObject(Object parsed) throws MarshalException {
      try {
         return new Constants.Value(this.fromString((String)parsed));
      } catch (ClassCastException var3) {
         throw new MarshalException(String.format("Expected a string representation of a timeuuid, but got a %s: %s", new Object[]{parsed.getClass().getSimpleName(), parsed}));
      }
   }

   public CQL3Type asCQL3Type() {
      return CQL3Type.Native.TIMEUUID;
   }

   public TypeSerializer<UUID> getSerializer() {
      return TimeUUIDSerializer.instance;
   }

   public long toTimeInMillis(ByteBuffer value) {
      return UUIDGen.unixTimestamp(UUIDGen.getUUID(value));
   }

   public ByteBuffer addDuration(Number temporal, Duration duration) {
      throw new UnsupportedOperationException();
   }

   public ByteBuffer substractDuration(Number temporal, Duration duration) {
      throw new UnsupportedOperationException();
   }

   public ByteBuffer now() {
      return ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes());
   }
}
