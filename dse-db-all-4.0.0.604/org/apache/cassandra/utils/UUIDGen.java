package org.apache.cassandra.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Ints;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.Iterator;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class UUIDGen {
   private static final long START_EPOCH = -12219292800000L;
   private static final long clockSeqAndNode = makeClockSeqAndNode();
   private static final long MIN_CLOCK_SEQ_AND_NODE = -9187201950435737472L;
   private static final long MAX_CLOCK_SEQ_AND_NODE = 9187201950435737471L;
   private static final SecureRandom secureRandom = new SecureRandom();
   private static final UUIDGen instance = new UUIDGen();
   private AtomicLong lastNanos = new AtomicLong();

   private UUIDGen() {
      if(clockSeqAndNode == 0L) {
         throw new RuntimeException("singleton instantiation is misplaced.");
      }
   }

   public static UUID getTimeUUID() {
      return new UUID(instance.createTimeSafe(), clockSeqAndNode);
   }

   public static UUID getTimeUUID(long when) {
      return new UUID(createTime(fromUnixTimestamp(when)), clockSeqAndNode);
   }

   public static UUID getTimeUUIDFromMicros(long whenInMicros) {
      long whenInMillis = whenInMicros / 1000L;
      long nanos = (whenInMicros - whenInMillis * 1000L) * 10L;
      return getTimeUUID(whenInMillis, nanos);
   }

   public static UUID getRandomTimeUUIDFromMicros(long whenInMicros) {
      long whenInMillis = whenInMicros / 1000L;
      long nanos = (whenInMicros - whenInMillis * 1000L) * 10L;
      return new UUID(createTime(fromUnixTimestamp(whenInMillis, nanos)), secureRandom.nextLong());
   }

   public static UUID getTimeUUID(long when, long nanos) {
      return new UUID(createTime(fromUnixTimestamp(when, nanos)), clockSeqAndNode);
   }

   @VisibleForTesting
   public static UUID getTimeUUID(long when, long nanos, long clockSeqAndNode) {
      return new UUID(createTime(fromUnixTimestamp(when, nanos)), clockSeqAndNode);
   }

   public static UUID getUUID(ByteBuffer raw) {
      return new UUID(raw.getLong(raw.position()), raw.getLong(raw.position() + 8));
   }

   public static ByteBuffer toByteBuffer(UUID uuid) {
      ByteBuffer buffer = ByteBuffer.allocate(16);
      buffer.putLong(uuid.getMostSignificantBits());
      buffer.putLong(uuid.getLeastSignificantBits());
      buffer.flip();
      return buffer;
   }

   public static byte[] decompose(UUID uuid) {
      long most = uuid.getMostSignificantBits();
      long least = uuid.getLeastSignificantBits();
      byte[] b = new byte[16];

      for(int i = 0; i < 8; ++i) {
         b[i] = (byte)((int)(most >>> (7 - i) * 8));
         b[8 + i] = (byte)((int)(least >>> (7 - i) * 8));
      }

      return b;
   }

   public static byte[] getTimeUUIDBytes() {
      return createTimeUUIDBytes(instance.createTimeSafe());
   }

   public static UUID minTimeUUID(long timestamp) {
      return new UUID(createTime(fromUnixTimestamp(timestamp)), -9187201950435737472L);
   }

   public static UUID maxTimeUUID(long timestamp) {
      long uuidTstamp = fromUnixTimestamp(timestamp + 1L) - 1L;
      return new UUID(createTime(uuidTstamp), 9187201950435737471L);
   }

   public static long unixTimestamp(UUID uuid) {
      return uuid.timestamp() / 10000L + -12219292800000L;
   }

   public static int unixTimestampInSec(UUID uuid) {
      return Ints.checkedCast(TimeUnit.MILLISECONDS.toSeconds(unixTimestamp(uuid)));
   }

   public static long microsTimestamp(UUID uuid) {
      return uuid.timestamp() / 10L + -12219292800000000L;
   }

   private static long fromUnixTimestamp(long timestamp) {
      return fromUnixTimestamp(timestamp, 0L);
   }

   private static long fromUnixTimestamp(long timestamp, long nanos) {
      return (timestamp - -12219292800000L) * 10000L + nanos;
   }

   public static byte[] getTimeUUIDBytes(long timeMillis, int nanos) {
      if(nanos >= 10000) {
         throw new IllegalArgumentException();
      } else {
         return createTimeUUIDBytes(instance.createTimeUnsafe(timeMillis, nanos));
      }
   }

   private static byte[] createTimeUUIDBytes(long msb) {
      long lsb = clockSeqAndNode;
      byte[] uuidBytes = new byte[16];

      int i;
      for(i = 0; i < 8; ++i) {
         uuidBytes[i] = (byte)((int)(msb >>> 8 * (7 - i)));
      }

      for(i = 8; i < 16; ++i) {
         uuidBytes[i] = (byte)((int)(lsb >>> 8 * (7 - i)));
      }

      return uuidBytes;
   }

   public static long getAdjustedTimestamp(UUID uuid) {
      if(uuid.version() != 1) {
         throw new IllegalArgumentException("incompatible with uuid version: " + uuid.version());
      } else {
         return uuid.timestamp() / 10000L + -12219292800000L;
      }
   }

   private static long makeClockSeqAndNode() {
      long clock = (new SecureRandom()).nextLong();
      long lsb = 0L;
      lsb |= -9223372036854775808L;
      lsb |= (clock & 16383L) << 48;
      lsb |= makeNode();
      return lsb;
   }

   private long createTimeSafe() {
      while(true) {
         long newLastNanos = (System.currentTimeMillis() - -12219292800000L) * 10000L;
         long originalLastNanos = this.lastNanos.get();
         if(newLastNanos > originalLastNanos) {
            if(!this.lastNanos.compareAndSet(originalLastNanos, newLastNanos)) {
               continue;
            }
         } else {
            newLastNanos = this.lastNanos.incrementAndGet();
         }

         return createTime(newLastNanos);
      }
   }

   private long createTimeUnsafe(long when, int nanos) {
      long nanosSince = (when - -12219292800000L) * 10000L + (long)nanos;
      return createTime(nanosSince);
   }

   private static long createTime(long nanosSince) {
      long msb = 0L;
      msb |= (4294967295L & nanosSince) << 32;
      msb |= (281470681743360L & nanosSince) >>> 16;
      msb |= (-281474976710656L & nanosSince) >>> 48;
      msb |= 4096L;
      return msb;
   }

   private static long makeNode() {
      Collection<InetAddress> localAddresses = FBUtilities.getAllLocalAddresses();
      if(localAddresses.isEmpty()) {
         throw new RuntimeException("Cannot generate the node component of the UUID because cannot retrieve any IP addresses.");
      } else {
         byte[] hash = hash(localAddresses);
         long node = 0L;

         for(int i = 0; i < Math.min(6, hash.length); ++i) {
            node |= (255L & (long)hash[i]) << (5 - i) * 8;
         }

         assert (-72057594037927936L & node) == 0L;

         return node | 1099511627776L;
      }
   }

   private static byte[] hash(Collection<InetAddress> data) {
      Hasher hasher = Hashing.md5().newHasher();
      Iterator var2 = data.iterator();

      while(var2.hasNext()) {
         InetAddress addr = (InetAddress)var2.next();
         hasher.putBytes(addr.getAddress());
      }

      long pid = NativeLibrary.getProcessID();
      if(pid < 0L) {
         pid = (new Random(System.currentTimeMillis())).nextLong();
      }

      HashingUtils.updateWithLong(hasher, pid);
      ClassLoader loader = UUIDGen.class.getClassLoader();
      int loaderId = loader != null?System.identityHashCode(loader):0;
      HashingUtils.updateWithInt(hasher, loaderId);
      return hasher.hash().asBytes();
   }
}
