package org.apache.cassandra.utils;

import java.nio.ByteBuffer;

public class MurmurHash {
   public MurmurHash() {
   }

   public static int hash32(ByteBuffer data, int offset, int length, int seed) {
      int m = 1540483477;
      int r = 24;
      int h = seed ^ length;
      int len_4 = length >> 2;

      int len_m;
      int left;
      for(len_m = 0; len_m < len_4; ++len_m) {
         left = len_m << 2;
         int k = data.get(offset + left + 3);
         k = k << 8;
         k |= data.get(offset + left + 2) & 255;
         k <<= 8;
         k |= data.get(offset + left + 1) & 255;
         k <<= 8;
         k |= data.get(offset + left + 0) & 255;
         k *= m;
         k ^= k >>> r;
         k *= m;
         h *= m;
         h ^= k;
      }

      len_m = len_4 << 2;
      left = length - len_m;
      if(left != 0) {
         if(left >= 3) {
            h ^= data.get(offset + length - 3) << 16;
         }

         if(left >= 2) {
            h ^= data.get(offset + length - 2) << 8;
         }

         if(left >= 1) {
            h ^= data.get(offset + length - 1);
         }

         h *= m;
      }

      h ^= h >>> 13;
      h *= m;
      h ^= h >>> 15;
      return h;
   }

   public static long hash2_64(ByteBuffer key, int offset, int length, long seed) {
      long m64 = -4132994306676758123L;
      int r64 = 47;
      long h64 = seed & 4294967295L ^ m64 * (long)length;
      int lenLongs = length >> 3;

      int rem;
      for(rem = 0; rem < lenLongs; ++rem) {
         int i_8 = rem << 3;
         long k64 = ((long)key.get(offset + i_8 + 0) & 255L) + (((long)key.get(offset + i_8 + 1) & 255L) << 8) + (((long)key.get(offset + i_8 + 2) & 255L) << 16) + (((long)key.get(offset + i_8 + 3) & 255L) << 24) + (((long)key.get(offset + i_8 + 4) & 255L) << 32) + (((long)key.get(offset + i_8 + 5) & 255L) << 40) + (((long)key.get(offset + i_8 + 6) & 255L) << 48) + (((long)key.get(offset + i_8 + 7) & 255L) << 56);
         k64 *= m64;
         k64 ^= k64 >>> r64;
         k64 *= m64;
         h64 ^= k64;
         h64 *= m64;
      }

      rem = length & 7;
      switch(rem) {
      case 7:
         h64 ^= (long)key.get(offset + length - rem + 6) << 48;
      case 6:
         h64 ^= (long)key.get(offset + length - rem + 5) << 40;
      case 5:
         h64 ^= (long)key.get(offset + length - rem + 4) << 32;
      case 4:
         h64 ^= (long)key.get(offset + length - rem + 3) << 24;
      case 3:
         h64 ^= (long)key.get(offset + length - rem + 2) << 16;
      case 2:
         h64 ^= (long)key.get(offset + length - rem + 1) << 8;
      case 1:
         h64 ^= (long)key.get(offset + length - rem);
         h64 *= m64;
      case 0:
      default:
         h64 ^= h64 >>> r64;
         h64 *= m64;
         h64 ^= h64 >>> r64;
         return h64;
      }
   }

   protected static long getblock(ByteBuffer key, int offset, int index) {
      int i_8 = index << 3;
      int blockOffset = offset + i_8;
      return ((long)key.get(blockOffset + 0) & 255L) + (((long)key.get(blockOffset + 1) & 255L) << 8) + (((long)key.get(blockOffset + 2) & 255L) << 16) + (((long)key.get(blockOffset + 3) & 255L) << 24) + (((long)key.get(blockOffset + 4) & 255L) << 32) + (((long)key.get(blockOffset + 5) & 255L) << 40) + (((long)key.get(blockOffset + 6) & 255L) << 48) + (((long)key.get(blockOffset + 7) & 255L) << 56);
   }

   protected static long rotl64(long v, int n) {
      return v << n | v >>> 64 - n;
   }

   protected static long fmix(long k) {
      k ^= k >>> 33;
      k *= -49064778989728563L;
      k ^= k >>> 33;
      k *= -4265267296055464877L;
      k ^= k >>> 33;
      return k;
   }

   public static void hash3_x64_128(ByteBuffer key, int offset, int length, long seed, long[] result) {
      int nblocks = length >> 4;
      long h1 = seed;
      long h2 = seed;
      long c1 = -8663945395140668459L;
      long c2 = 5545529020109919103L;

      for(int i = 0; i < nblocks; ++i) {
         long k1 = getblock(key, offset, i * 2 + 0);
         long k2 = getblock(key, offset, i * 2 + 1);
         k1 *= c1;
         k1 = rotl64(k1, 31);
         k1 *= c2;
         h1 ^= k1;
         h1 = rotl64(h1, 27);
         h1 += h2;
         h1 = h1 * 5L + 1390208809L;
         k2 *= c2;
         k2 = rotl64(k2, 33);
         k2 *= c1;
         h2 ^= k2;
         h2 = rotl64(h2, 31);
         h2 += h1;
         h2 = h2 * 5L + 944331445L;
      }

      offset += nblocks * 16;
      long k1 = 0L;
      long k2 = 0L;
      switch(length & 15) {
      case 15:
         k2 ^= (long)key.get(offset + 14) << 48;
      case 14:
         k2 ^= (long)key.get(offset + 13) << 40;
      case 13:
         k2 ^= (long)key.get(offset + 12) << 32;
      case 12:
         k2 ^= (long)key.get(offset + 11) << 24;
      case 11:
         k2 ^= (long)key.get(offset + 10) << 16;
      case 10:
         k2 ^= (long)key.get(offset + 9) << 8;
      case 9:
         k2 ^= (long)key.get(offset + 8) << 0;
         k2 *= c2;
         k2 = rotl64(k2, 33);
         k2 *= c1;
         h2 ^= k2;
      case 8:
         k1 ^= (long)key.get(offset + 7) << 56;
      case 7:
         k1 ^= (long)key.get(offset + 6) << 48;
      case 6:
         k1 ^= (long)key.get(offset + 5) << 40;
      case 5:
         k1 ^= (long)key.get(offset + 4) << 32;
      case 4:
         k1 ^= (long)key.get(offset + 3) << 24;
      case 3:
         k1 ^= (long)key.get(offset + 2) << 16;
      case 2:
         k1 ^= (long)key.get(offset + 1) << 8;
      case 1:
         k1 ^= (long)key.get(offset);
         k1 *= c1;
         k1 = rotl64(k1, 31);
         k1 *= c2;
         h1 ^= k1;
      default:
         h1 ^= (long)length;
         h2 ^= (long)length;
         h1 += h2;
         h2 += h1;
         h1 = fmix(h1);
         h2 = fmix(h2);
         h1 += h2;
         h2 += h1;
         result[0] = h1;
         result[1] = h2;
      }
   }
}
