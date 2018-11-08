package com.datastax.bdp.util;

public class Isaac {
   static final int SIZEL = 8;
   static final int SIZE = 256;
   static final int MASK = 1020;
   int count;
   int[] rsl = new int[256];
   private int[] mem = new int[256];
   private int a;
   private int b;
   private int c;

   public Isaac() {
      this.init(false);
   }

   public Isaac(int[] seed) {
      System.arraycopy(seed, 0, this.rsl, 0, seed.length);
      this.init(true);
   }

   public final void isaac() {
      this.b += ++this.c;
      int i = 0;

      int j;
      int x;
      int y;
      for(j = 128; i < 128; this.rsl[i++] = this.b = this.mem[(y >> 8 & 1020) >> 2] + x) {
         x = this.mem[i];
         this.a ^= this.a << 13;
         this.a += this.mem[j++];
         this.mem[i] = y = this.mem[(x & 1020) >> 2] + this.a + this.b;
         this.rsl[i++] = this.b = this.mem[(y >> 8 & 1020) >> 2] + x;
         x = this.mem[i];
         this.a ^= this.a >>> 6;
         this.a += this.mem[j++];
         this.mem[i] = y = this.mem[(x & 1020) >> 2] + this.a + this.b;
         this.rsl[i++] = this.b = this.mem[(y >> 8 & 1020) >> 2] + x;
         x = this.mem[i];
         this.a ^= this.a << 2;
         this.a += this.mem[j++];
         this.mem[i] = y = this.mem[(x & 1020) >> 2] + this.a + this.b;
         this.rsl[i++] = this.b = this.mem[(y >> 8 & 1020) >> 2] + x;
         x = this.mem[i];
         this.a ^= this.a >>> 16;
         this.a += this.mem[j++];
         this.mem[i] = y = this.mem[(x & 1020) >> 2] + this.a + this.b;
      }

      for(j = 0; j < 128; this.rsl[i++] = this.b = this.mem[(y >> 8 & 1020) >> 2] + x) {
         x = this.mem[i];
         this.a ^= this.a << 13;
         this.a += this.mem[j++];
         this.mem[i] = y = this.mem[(x & 1020) >> 2] + this.a + this.b;
         this.rsl[i++] = this.b = this.mem[(y >> 8 & 1020) >> 2] + x;
         x = this.mem[i];
         this.a ^= this.a >>> 6;
         this.a += this.mem[j++];
         this.mem[i] = y = this.mem[(x & 1020) >> 2] + this.a + this.b;
         this.rsl[i++] = this.b = this.mem[(y >> 8 & 1020) >> 2] + x;
         x = this.mem[i];
         this.a ^= this.a << 2;
         this.a += this.mem[j++];
         this.mem[i] = y = this.mem[(x & 1020) >> 2] + this.a + this.b;
         this.rsl[i++] = this.b = this.mem[(y >> 8 & 1020) >> 2] + x;
         x = this.mem[i];
         this.a ^= this.a >>> 16;
         this.a += this.mem[j++];
         this.mem[i] = y = this.mem[(x & 1020) >> 2] + this.a + this.b;
      }

   }

   public final void init(boolean flag) {
      int h = -1640531527;
      int g = -1640531527;
      int f = -1640531527;
      int e = -1640531527;
      int d = -1640531527;
      int c = -1640531527;
      int b = -1640531527;
      int a = -1640531527;

      int i;
      for(i = 0; i < 4; ++i) {
         a ^= b << 11;
         d += a;
         b += c;
         b ^= c >>> 2;
         e += b;
         c += d;
         c ^= d << 8;
         f += c;
         d += e;
         d ^= e >>> 16;
         g += d;
         e += f;
         e ^= f << 10;
         h += e;
         f += g;
         f ^= g >>> 4;
         a += f;
         g += h;
         g ^= h << 8;
         b += g;
         h += a;
         h ^= a >>> 9;
         c += h;
         a += b;
      }

      for(i = 0; i < 256; i += 8) {
         if(flag) {
            a += this.rsl[i];
            b += this.rsl[i + 1];
            c += this.rsl[i + 2];
            d += this.rsl[i + 3];
            e += this.rsl[i + 4];
            f += this.rsl[i + 5];
            g += this.rsl[i + 6];
            h += this.rsl[i + 7];
         }

         a ^= b << 11;
         d += a;
         b += c;
         b ^= c >>> 2;
         e += b;
         c += d;
         c ^= d << 8;
         f += c;
         d += e;
         d ^= e >>> 16;
         g += d;
         e += f;
         e ^= f << 10;
         h += e;
         f += g;
         f ^= g >>> 4;
         a += f;
         g += h;
         g ^= h << 8;
         b += g;
         h += a;
         h ^= a >>> 9;
         c += h;
         a += b;
         this.mem[i] = a;
         this.mem[i + 1] = b;
         this.mem[i + 2] = c;
         this.mem[i + 3] = d;
         this.mem[i + 4] = e;
         this.mem[i + 5] = f;
         this.mem[i + 6] = g;
         this.mem[i + 7] = h;
      }

      if(flag) {
         for(i = 0; i < 256; i += 8) {
            a += this.mem[i];
            b += this.mem[i + 1];
            c += this.mem[i + 2];
            d += this.mem[i + 3];
            e += this.mem[i + 4];
            f += this.mem[i + 5];
            g += this.mem[i + 6];
            h += this.mem[i + 7];
            a ^= b << 11;
            d += a;
            b += c;
            b ^= c >>> 2;
            e += b;
            c += d;
            c ^= d << 8;
            f += c;
            d += e;
            d ^= e >>> 16;
            g += d;
            e += f;
            e ^= f << 10;
            h += e;
            f += g;
            f ^= g >>> 4;
            a += f;
            g += h;
            g ^= h << 8;
            b += g;
            h += a;
            h ^= a >>> 9;
            c += h;
            a += b;
            this.mem[i] = a;
            this.mem[i + 1] = b;
            this.mem[i + 2] = c;
            this.mem[i + 3] = d;
            this.mem[i + 4] = e;
            this.mem[i + 5] = f;
            this.mem[i + 6] = g;
            this.mem[i + 7] = h;
         }
      }

      this.isaac();
      this.count = 256;
   }

   public final int nextInt() {
      if(0 == this.count--) {
         this.isaac();
         this.count = 255;
      }

      return this.rsl[this.count];
   }

   public static void main(String[] args) {
      int[] seed = new int[256];
      Isaac x = new Isaac(seed);

      for(int i = 0; i < 2; ++i) {
         x.isaac();

         for(int j = 0; j < 256; ++j) {
            String z;
            for(z = Integer.toHexString(x.rsl[j]); z.length() < 8; z = "0" + z) {
               ;
            }

            System.out.print(z);
            if((j & 7) == 7) {
               System.out.println("");
            }
         }
      }

   }
}
