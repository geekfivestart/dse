package org.apache.cassandra.utils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.function.Supplier;

public class MD5Digest {
   private static final Supplier<MessageDigest> localMD5Digest = HashingUtils.newThreadLocalMessageDigest("MD5");
   public final byte[] bytes;
   private final int hashCode;

   private MD5Digest(byte[] bytes) {
      this.bytes = bytes;
      this.hashCode = Arrays.hashCode(bytes);
   }

   public static MD5Digest wrap(byte[] digest) {
      return new MD5Digest(digest);
   }

   public static MD5Digest compute(byte[] toHash) {
      return new MD5Digest(((MessageDigest)localMD5Digest.get()).digest(toHash));
   }

   public static MD5Digest compute(String toHash) {
      return compute(toHash.getBytes(StandardCharsets.UTF_8));
   }

   public ByteBuffer byteBuffer() {
      return ByteBuffer.wrap(this.bytes);
   }

   public final int hashCode() {
      return this.hashCode;
   }

   public final boolean equals(Object o) {
      if(!(o instanceof MD5Digest)) {
         return false;
      } else {
         MD5Digest that = (MD5Digest)o;
         return FBUtilities.compareUnsigned(this.bytes, that.bytes, 0, 0, this.bytes.length, that.bytes.length) == 0;
      }
   }

   public String toString() {
      return Hex.bytesToHex(this.bytes);
   }

   public static MessageDigest threadLocalMD5Digest() {
      return (MessageDigest)localMD5Digest.get();
   }
}
