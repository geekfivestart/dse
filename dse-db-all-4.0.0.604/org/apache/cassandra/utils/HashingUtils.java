package org.apache.cassandra.utils;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import io.netty.util.concurrent.FastThreadLocal;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.function.Supplier;

public class HashingUtils {
   public static final HashFunction CURRENT_HASH_FUNCTION = Hashing.md5();

   public HashingUtils() {
   }

   public static Supplier<MessageDigest> newThreadLocalMessageDigest(final String algorithm) {
      try {
         MessageDigest.getInstance(algorithm);
         FastThreadLocal<MessageDigest> threadLocal = new FastThreadLocal<MessageDigest>() {
            protected MessageDigest initialValue() throws Exception {
               return MessageDigest.getInstance(algorithm);
            }
         };
         return () -> {
            MessageDigest digest = (MessageDigest)threadLocal.get();
            digest.reset();
            return digest;
         };
      } catch (NoSuchAlgorithmException var2) {
         throw new RuntimeException("the requested digest algorithm (" + algorithm + ") is not available", var2);
      }
   }

   public static void updateBytes(Hasher hasher, ByteBuffer input) {
      if(input.hasRemaining()) {
         int n;
         int chunk;
         if(input.hasArray()) {
            byte[] b = input.array();
            n = input.arrayOffset();
            int pos = input.position();
            chunk = input.limit();
            hasher.putBytes(b, n + pos, chunk - pos);
            input.position(chunk);
         } else {
            int len = input.remaining();
            n = Math.min(len, 4096);

            for(byte[] tempArray = new byte[n]; len > 0; len -= chunk) {
               chunk = Math.min(len, tempArray.length);
               input.get(tempArray, 0, chunk);
               hasher.putBytes(tempArray, 0, chunk);
            }
         }

      }
   }

   public static void updateWithShort(Hasher hasher, int val) {
      hasher.putByte((byte)(val >> 8 & 255));
      hasher.putByte((byte)(val & 255));
   }

   public static void updateWithByte(Hasher hasher, int val) {
      hasher.putByte((byte)(val & 255));
   }

   public static void updateWithInt(Hasher hasher, int val) {
      hasher.putByte((byte)(val >>> 24 & 255));
      hasher.putByte((byte)(val >>> 16 & 255));
      hasher.putByte((byte)(val >>> 8 & 255));
      hasher.putByte((byte)(val >>> 0 & 255));
   }

   public static void updateWithLong(Hasher hasher, long val) {
      hasher.putByte((byte)((int)(val >>> 56 & 255L)));
      hasher.putByte((byte)((int)(val >>> 48 & 255L)));
      hasher.putByte((byte)((int)(val >>> 40 & 255L)));
      hasher.putByte((byte)((int)(val >>> 32 & 255L)));
      hasher.putByte((byte)((int)(val >>> 24 & 255L)));
      hasher.putByte((byte)((int)(val >>> 16 & 255L)));
      hasher.putByte((byte)((int)(val >>> 8 & 255L)));
      hasher.putByte((byte)((int)(val >>> 0 & 255L)));
   }

   public static void updateWithBoolean(Hasher hasher, boolean val) {
      updateWithByte(hasher, val?0:1);
   }
}
