package org.apache.cassandra.utils;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.cassandra.db.SystemKeyspace;

public class CounterId implements Comparable<CounterId> {
   public static final int LENGTH = 16;
   private int hashCode = -1;
   private final ByteBuffer id;

   private static CounterId.LocalCounterIdHolder localId() {
      return CounterId.LocalId.instance;
   }

   public static CounterId getLocalId() {
      return localId().get();
   }

   public static CounterId fromInt(int n) {
      long lowBits = -4611686018427387904L | (long)n;
      return new CounterId(ByteBuffer.allocate(16).putLong(0, 0L).putLong(8, lowBits));
   }

   public static CounterId wrap(ByteBuffer id) {
      return new CounterId(id);
   }

   public static CounterId wrap(ByteBuffer bb, int offset) {
      ByteBuffer dup = bb.duplicate();
      dup.position(offset);
      dup.limit(dup.position() + 16);
      return wrap(dup);
   }

   private CounterId(ByteBuffer id) {
      if(id.remaining() != 16) {
         throw new IllegalArgumentException("A CounterId representation is exactly 16 bytes");
      } else {
         this.id = id;
      }
   }

   public static CounterId generate() {
      return new CounterId(ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes()));
   }

   public ByteBuffer bytes() {
      return this.id;
   }

   public boolean isLocalId() {
      return this.equals(getLocalId());
   }

   public int compareTo(CounterId o) {
      return ByteBufferUtil.compareSubArrays(this.id, this.id.position(), o.id, o.id.position(), 16);
   }

   public String toString() {
      return UUIDGen.getUUID(this.id).toString();
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         CounterId otherId = (CounterId)o;
         return this.id.equals(otherId.id);
      } else {
         return false;
      }
   }

   public int hashCode() {
      int currHashCode = this.hashCode;
      if(currHashCode == -1) {
         currHashCode = this.id.hashCode();
         this.hashCode = currHashCode;
      }

      return currHashCode;
   }

   private static class LocalCounterIdHolder {
      private final AtomicReference<CounterId> current = new AtomicReference(CounterId.wrap(ByteBufferUtil.bytes(SystemKeyspace.getLocalHostId())));

      LocalCounterIdHolder() {
      }

      CounterId get() {
         return (CounterId)this.current.get();
      }
   }

   private static class LocalId {
      static final CounterId.LocalCounterIdHolder instance = new CounterId.LocalCounterIdHolder();

      private LocalId() {
      }
   }
}
