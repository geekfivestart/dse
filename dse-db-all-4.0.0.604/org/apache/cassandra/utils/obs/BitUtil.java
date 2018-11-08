package org.apache.cassandra.utils.obs;

final class BitUtil {
   public static final byte[] ntzTable = new byte[]{8, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 7, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0};

   BitUtil() {
   }

   public static int pop(long x) {
      x -= x >>> 1 & 6148914691236517205L;
      x = (x & 3689348814741910323L) + (x >>> 2 & 3689348814741910323L);
      x = x + (x >>> 4) & 1085102592571150095L;
      x += x >>> 8;
      x += x >>> 16;
      x += x >>> 32;
      return (int)x & 127;
   }

   public static long pop_array(long[] A, int wordOffset, int numWords) {
      int n = wordOffset + numWords;
      long tot = 0L;
      long tot8 = 0L;
      long ones = 0L;
      long twos = 0L;
      long fours = 0L;

      int i;
      long b;
      long c;
      long u;
      long twosA;
      long foursA;
      long eights;
      long u;
      for(i = wordOffset; i <= n - 8; i += 8) {
         eights = A[i];
         u = A[i + 1];
         long u = ones ^ eights;
         b = ones & eights | u & u;
         ones = u ^ u;
         eights = A[i + 2];
         u = A[i + 3];
         u = ones ^ eights;
         c = ones & eights | u & u;
         ones = u ^ u;
         eights = twos ^ b;
         u = twos & b | eights & c;
         twos = eights ^ c;
         eights = A[i + 4];
         u = A[i + 5];
         u = ones ^ eights;
         b = ones & eights | u & u;
         ones = u ^ u;
         eights = A[i + 6];
         u = A[i + 7];
         u = ones ^ eights;
         c = ones & eights | u & u;
         ones = u ^ u;
         eights = twos ^ b;
         twosA = twos & b | eights & c;
         twos = eights ^ c;
         eights = fours ^ u;
         foursA = fours & u | eights & twosA;
         fours = eights ^ twosA;
         tot8 += (long)pop(foursA);
      }

      if(i <= n - 4) {
         foursA = A[i];
         eights = A[i + 1];
         u = ones ^ foursA;
         b = ones & foursA | u & eights;
         ones = u ^ eights;
         foursA = A[i + 2];
         eights = A[i + 3];
         u = ones ^ foursA;
         c = ones & foursA | u & eights;
         ones = u ^ eights;
         foursA = twos ^ b;
         u = twos & b | foursA & c;
         twos = foursA ^ c;
         twosA = fours & u;
         fours ^= u;
         tot8 += (long)pop(twosA);
         i += 4;
      }

      if(i <= n - 2) {
         b = A[i];
         c = A[i + 1];
         u = ones ^ b;
         twosA = ones & b | u & c;
         ones = u ^ c;
         foursA = twos & twosA;
         twos ^= twosA;
         eights = fours & foursA;
         fours ^= foursA;
         tot8 += (long)pop(eights);
         i += 2;
      }

      if(i < n) {
         tot += (long)pop(A[i]);
      }

      tot += (long)((pop(fours) << 2) + (pop(twos) << 1) + pop(ones)) + (tot8 << 3);
      return tot;
   }

   public static long pop_intersect(long[] A, long[] B, int wordOffset, int numWords) {
      int n = wordOffset + numWords;
      long tot = 0L;
      long tot8 = 0L;
      long ones = 0L;
      long twos = 0L;
      long fours = 0L;

      int i;
      long b;
      long c;
      long u;
      long twosA;
      long foursA;
      long eights;
      long u;
      for(i = wordOffset; i <= n - 8; i += 8) {
         eights = A[i] & B[i];
         u = A[i + 1] & B[i + 1];
         long u = ones ^ eights;
         b = ones & eights | u & u;
         ones = u ^ u;
         eights = A[i + 2] & B[i + 2];
         u = A[i + 3] & B[i + 3];
         u = ones ^ eights;
         c = ones & eights | u & u;
         ones = u ^ u;
         eights = twos ^ b;
         u = twos & b | eights & c;
         twos = eights ^ c;
         eights = A[i + 4] & B[i + 4];
         u = A[i + 5] & B[i + 5];
         u = ones ^ eights;
         b = ones & eights | u & u;
         ones = u ^ u;
         eights = A[i + 6] & B[i + 6];
         u = A[i + 7] & B[i + 7];
         u = ones ^ eights;
         c = ones & eights | u & u;
         ones = u ^ u;
         eights = twos ^ b;
         twosA = twos & b | eights & c;
         twos = eights ^ c;
         eights = fours ^ u;
         foursA = fours & u | eights & twosA;
         fours = eights ^ twosA;
         tot8 += (long)pop(foursA);
      }

      if(i <= n - 4) {
         foursA = A[i] & B[i];
         eights = A[i + 1] & B[i + 1];
         u = ones ^ foursA;
         b = ones & foursA | u & eights;
         ones = u ^ eights;
         foursA = A[i + 2] & B[i + 2];
         eights = A[i + 3] & B[i + 3];
         u = ones ^ foursA;
         c = ones & foursA | u & eights;
         ones = u ^ eights;
         foursA = twos ^ b;
         u = twos & b | foursA & c;
         twos = foursA ^ c;
         twosA = fours & u;
         fours ^= u;
         tot8 += (long)pop(twosA);
         i += 4;
      }

      if(i <= n - 2) {
         b = A[i] & B[i];
         c = A[i + 1] & B[i + 1];
         u = ones ^ b;
         twosA = ones & b | u & c;
         ones = u ^ c;
         foursA = twos & twosA;
         twos ^= twosA;
         eights = fours & foursA;
         fours ^= foursA;
         tot8 += (long)pop(eights);
         i += 2;
      }

      if(i < n) {
         tot += (long)pop(A[i] & B[i]);
      }

      tot += (long)((pop(fours) << 2) + (pop(twos) << 1) + pop(ones)) + (tot8 << 3);
      return tot;
   }

   public static long pop_union(long[] A, long[] B, int wordOffset, int numWords) {
      int n = wordOffset + numWords;
      long tot = 0L;
      long tot8 = 0L;
      long ones = 0L;
      long twos = 0L;
      long fours = 0L;

      int i;
      long b;
      long c;
      long u;
      long twosA;
      long foursA;
      long eights;
      long u;
      for(i = wordOffset; i <= n - 8; i += 8) {
         eights = A[i] | B[i];
         u = A[i + 1] | B[i + 1];
         long u = ones ^ eights;
         b = ones & eights | u & u;
         ones = u ^ u;
         eights = A[i + 2] | B[i + 2];
         u = A[i + 3] | B[i + 3];
         u = ones ^ eights;
         c = ones & eights | u & u;
         ones = u ^ u;
         eights = twos ^ b;
         u = twos & b | eights & c;
         twos = eights ^ c;
         eights = A[i + 4] | B[i + 4];
         u = A[i + 5] | B[i + 5];
         u = ones ^ eights;
         b = ones & eights | u & u;
         ones = u ^ u;
         eights = A[i + 6] | B[i + 6];
         u = A[i + 7] | B[i + 7];
         u = ones ^ eights;
         c = ones & eights | u & u;
         ones = u ^ u;
         eights = twos ^ b;
         twosA = twos & b | eights & c;
         twos = eights ^ c;
         eights = fours ^ u;
         foursA = fours & u | eights & twosA;
         fours = eights ^ twosA;
         tot8 += (long)pop(foursA);
      }

      if(i <= n - 4) {
         foursA = A[i] | B[i];
         eights = A[i + 1] | B[i + 1];
         u = ones ^ foursA;
         b = ones & foursA | u & eights;
         ones = u ^ eights;
         foursA = A[i + 2] | B[i + 2];
         eights = A[i + 3] | B[i + 3];
         u = ones ^ foursA;
         c = ones & foursA | u & eights;
         ones = u ^ eights;
         foursA = twos ^ b;
         u = twos & b | foursA & c;
         twos = foursA ^ c;
         twosA = fours & u;
         fours ^= u;
         tot8 += (long)pop(twosA);
         i += 4;
      }

      if(i <= n - 2) {
         b = A[i] | B[i];
         c = A[i + 1] | B[i + 1];
         u = ones ^ b;
         twosA = ones & b | u & c;
         ones = u ^ c;
         foursA = twos & twosA;
         twos ^= twosA;
         eights = fours & foursA;
         fours ^= foursA;
         tot8 += (long)pop(eights);
         i += 2;
      }

      if(i < n) {
         tot += (long)pop(A[i] | B[i]);
      }

      tot += (long)((pop(fours) << 2) + (pop(twos) << 1) + pop(ones)) + (tot8 << 3);
      return tot;
   }

   public static long pop_andnot(long[] A, long[] B, int wordOffset, int numWords) {
      int n = wordOffset + numWords;
      long tot = 0L;
      long tot8 = 0L;
      long ones = 0L;
      long twos = 0L;
      long fours = 0L;

      int i;
      long b;
      long c;
      long u;
      long twosA;
      long foursA;
      long eights;
      long u;
      for(i = wordOffset; i <= n - 8; i += 8) {
         eights = A[i] & ~B[i];
         u = A[i + 1] & ~B[i + 1];
         long u = ones ^ eights;
         b = ones & eights | u & u;
         ones = u ^ u;
         eights = A[i + 2] & ~B[i + 2];
         u = A[i + 3] & ~B[i + 3];
         u = ones ^ eights;
         c = ones & eights | u & u;
         ones = u ^ u;
         eights = twos ^ b;
         u = twos & b | eights & c;
         twos = eights ^ c;
         eights = A[i + 4] & ~B[i + 4];
         u = A[i + 5] & ~B[i + 5];
         u = ones ^ eights;
         b = ones & eights | u & u;
         ones = u ^ u;
         eights = A[i + 6] & ~B[i + 6];
         u = A[i + 7] & ~B[i + 7];
         u = ones ^ eights;
         c = ones & eights | u & u;
         ones = u ^ u;
         eights = twos ^ b;
         twosA = twos & b | eights & c;
         twos = eights ^ c;
         eights = fours ^ u;
         foursA = fours & u | eights & twosA;
         fours = eights ^ twosA;
         tot8 += (long)pop(foursA);
      }

      if(i <= n - 4) {
         foursA = A[i] & ~B[i];
         eights = A[i + 1] & ~B[i + 1];
         u = ones ^ foursA;
         b = ones & foursA | u & eights;
         ones = u ^ eights;
         foursA = A[i + 2] & ~B[i + 2];
         eights = A[i + 3] & ~B[i + 3];
         u = ones ^ foursA;
         c = ones & foursA | u & eights;
         ones = u ^ eights;
         foursA = twos ^ b;
         u = twos & b | foursA & c;
         twos = foursA ^ c;
         twosA = fours & u;
         fours ^= u;
         tot8 += (long)pop(twosA);
         i += 4;
      }

      if(i <= n - 2) {
         b = A[i] & ~B[i];
         c = A[i + 1] & ~B[i + 1];
         u = ones ^ b;
         twosA = ones & b | u & c;
         ones = u ^ c;
         foursA = twos & twosA;
         twos ^= twosA;
         eights = fours & foursA;
         fours ^= foursA;
         tot8 += (long)pop(eights);
         i += 2;
      }

      if(i < n) {
         tot += (long)pop(A[i] & ~B[i]);
      }

      tot += (long)((pop(fours) << 2) + (pop(twos) << 1) + pop(ones)) + (tot8 << 3);
      return tot;
   }

   public static long pop_xor(long[] A, long[] B, int wordOffset, int numWords) {
      int n = wordOffset + numWords;
      long tot = 0L;
      long tot8 = 0L;
      long ones = 0L;
      long twos = 0L;
      long fours = 0L;

      int i;
      long b;
      long c;
      long u;
      long twosA;
      long foursA;
      long eights;
      long u;
      for(i = wordOffset; i <= n - 8; i += 8) {
         eights = A[i] ^ B[i];
         u = A[i + 1] ^ B[i + 1];
         long u = ones ^ eights;
         b = ones & eights | u & u;
         ones = u ^ u;
         eights = A[i + 2] ^ B[i + 2];
         u = A[i + 3] ^ B[i + 3];
         u = ones ^ eights;
         c = ones & eights | u & u;
         ones = u ^ u;
         eights = twos ^ b;
         u = twos & b | eights & c;
         twos = eights ^ c;
         eights = A[i + 4] ^ B[i + 4];
         u = A[i + 5] ^ B[i + 5];
         u = ones ^ eights;
         b = ones & eights | u & u;
         ones = u ^ u;
         eights = A[i + 6] ^ B[i + 6];
         u = A[i + 7] ^ B[i + 7];
         u = ones ^ eights;
         c = ones & eights | u & u;
         ones = u ^ u;
         eights = twos ^ b;
         twosA = twos & b | eights & c;
         twos = eights ^ c;
         eights = fours ^ u;
         foursA = fours & u | eights & twosA;
         fours = eights ^ twosA;
         tot8 += (long)pop(foursA);
      }

      if(i <= n - 4) {
         foursA = A[i] ^ B[i];
         eights = A[i + 1] ^ B[i + 1];
         u = ones ^ foursA;
         b = ones & foursA | u & eights;
         ones = u ^ eights;
         foursA = A[i + 2] ^ B[i + 2];
         eights = A[i + 3] ^ B[i + 3];
         u = ones ^ foursA;
         c = ones & foursA | u & eights;
         ones = u ^ eights;
         foursA = twos ^ b;
         u = twos & b | foursA & c;
         twos = foursA ^ c;
         twosA = fours & u;
         fours ^= u;
         tot8 += (long)pop(twosA);
         i += 4;
      }

      if(i <= n - 2) {
         b = A[i] ^ B[i];
         c = A[i + 1] ^ B[i + 1];
         u = ones ^ b;
         twosA = ones & b | u & c;
         ones = u ^ c;
         foursA = twos & twosA;
         twos ^= twosA;
         eights = fours & foursA;
         fours ^= foursA;
         tot8 += (long)pop(eights);
         i += 2;
      }

      if(i < n) {
         tot += (long)pop(A[i] ^ B[i]);
      }

      tot += (long)((pop(fours) << 2) + (pop(twos) << 1) + pop(ones)) + (tot8 << 3);
      return tot;
   }

   public static int ntz(long val) {
      int lower = (int)val;
      int lowByte = lower & 255;
      if(lowByte != 0) {
         return ntzTable[lowByte];
      } else if(lower != 0) {
         lowByte = lower >>> 8 & 255;
         if(lowByte != 0) {
            return ntzTable[lowByte] + 8;
         } else {
            lowByte = lower >>> 16 & 255;
            return lowByte != 0?ntzTable[lowByte] + 16:ntzTable[lower >>> 24] + 24;
         }
      } else {
         int upper = (int)(val >> 32);
         lowByte = upper & 255;
         if(lowByte != 0) {
            return ntzTable[lowByte] + 32;
         } else {
            lowByte = upper >>> 8 & 255;
            if(lowByte != 0) {
               return ntzTable[lowByte] + 40;
            } else {
               lowByte = upper >>> 16 & 255;
               return lowByte != 0?ntzTable[lowByte] + 48:ntzTable[upper >>> 24] + 56;
            }
         }
      }
   }

   public static int ntz(int val) {
      int lowByte = val & 255;
      if(lowByte != 0) {
         return ntzTable[lowByte];
      } else {
         lowByte = val >>> 8 & 255;
         if(lowByte != 0) {
            return ntzTable[lowByte] + 8;
         } else {
            lowByte = val >>> 16 & 255;
            return lowByte != 0?ntzTable[lowByte] + 16:ntzTable[val >>> 24] + 24;
         }
      }
   }

   public static int ntz2(long x) {
      int n = 0;
      int y = (int)x;
      if(y == 0) {
         n += 32;
         y = (int)(x >>> 32);
      }

      if((y & '\uffff') == 0) {
         n += 16;
         y >>>= 16;
      }

      if((y & 255) == 0) {
         n += 8;
         y >>>= 8;
      }

      return ntzTable[y & 255] + n;
   }

   public static int ntz3(long x) {
      int n = 1;
      int y = (int)x;
      if(y == 0) {
         n += 32;
         y = (int)(x >>> 32);
      }

      if((y & '\uffff') == 0) {
         n += 16;
         y >>>= 16;
      }

      if((y & 255) == 0) {
         n += 8;
         y >>>= 8;
      }

      if((y & 15) == 0) {
         n += 4;
         y >>>= 4;
      }

      if((y & 3) == 0) {
         n += 2;
         y >>>= 2;
      }

      return n - (y & 1);
   }

   public static boolean isPowerOfTwo(int v) {
      return (v & v - 1) == 0;
   }

   public static boolean isPowerOfTwo(long v) {
      return (v & v - 1L) == 0L;
   }

   public static int nextHighestPowerOfTwo(int v) {
      --v;
      v |= v >> 1;
      v |= v >> 2;
      v |= v >> 4;
      v |= v >> 8;
      v |= v >> 16;
      ++v;
      return v;
   }

   public static long nextHighestPowerOfTwo(long v) {
      --v;
      v |= v >> 1;
      v |= v >> 2;
      v |= v >> 4;
      v |= v >> 8;
      v |= v >> 16;
      v |= v >> 32;
      ++v;
      return v;
   }
}
