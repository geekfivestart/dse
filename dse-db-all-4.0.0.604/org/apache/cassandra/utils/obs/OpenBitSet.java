package org.apache.cassandra.utils.obs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.utils.concurrent.Ref;

public class OpenBitSet implements IBitSet {
   private final long[][] bits;
   private int wlen;
   private final int pageCount;
   private static final int PAGE_SIZE = 4096;

   public OpenBitSet(long numBits) {
      this.wlen = (int)bits2words(numBits);
      int lastPageSize = this.wlen % 4096;
      int fullPageCount = this.wlen / 4096;
      this.pageCount = fullPageCount + (lastPageSize == 0?0:1);
      this.bits = new long[this.pageCount][];

      for(int i = 0; i < fullPageCount; ++i) {
         this.bits[i] = new long[4096];
      }

      if(lastPageSize != 0) {
         this.bits[this.bits.length - 1] = new long[lastPageSize];
      }

   }

   public OpenBitSet() {
      this(64L);
   }

   public int getPageSize() {
      return 4096;
   }

   public int getPageCount() {
      return this.pageCount;
   }

   public long[] getPage(int pageIdx) {
      return this.bits[pageIdx];
   }

   public long capacity() {
      return (long)this.wlen << 6;
   }

   public long offHeapSize() {
      return 0L;
   }

   public void addTo(Ref.IdentityCollection identities) {
   }

   public long size() {
      return this.capacity();
   }

   public long length() {
      return this.capacity();
   }

   public boolean isEmpty() {
      return this.cardinality() == 0L;
   }

   public int getNumWords() {
      return this.wlen;
   }

   public boolean get(int index) {
      int i = index >> 6;
      int bit = index & 63;
      long bitmask = 1L << bit;
      return (this.bits[i / 4096][i % 4096] & bitmask) != 0L;
   }

   public boolean get(long index) {
      int i = (int)(index >> 6);
      int bit = (int)index & 63;
      long bitmask = 1L << bit;
      return (this.bits[i / 4096][i % 4096] & bitmask) != 0L;
   }

   public void set(long index) {
      int wordNum = (int)(index >> 6);
      int bit = (int)index & 63;
      long bitmask = 1L << bit;
      this.bits[wordNum / 4096][wordNum % 4096] |= bitmask;
   }

   public void set(int index) {
      int wordNum = index >> 6;
      int bit = index & 63;
      long bitmask = 1L << bit;
      this.bits[wordNum / 4096][wordNum % 4096] |= bitmask;
   }

   public void clear(int index) {
      int wordNum = index >> 6;
      int bit = index & 63;
      long bitmask = 1L << bit;
      this.bits[wordNum / 4096][wordNum % 4096] &= ~bitmask;
   }

   public void clear(long index) {
      int wordNum = (int)(index >> 6);
      int bit = (int)index & 63;
      long bitmask = 1L << bit;
      this.bits[wordNum / 4096][wordNum % 4096] &= ~bitmask;
   }

   public void clear(int startIndex, int endIndex) {
      if(endIndex > startIndex) {
         int startWord = startIndex >> 6;
         if(startWord < this.wlen) {
            int endWord = endIndex - 1 >> 6;
            long startmask = -1L << startIndex;
            long endmask = -1L >>> -endIndex;
            startmask = ~startmask;
            endmask = ~endmask;
            if(startWord == endWord) {
               this.bits[startWord / 4096][startWord % 4096] &= startmask | endmask;
            } else {
               this.bits[startWord / 4096][startWord % 4096] &= startmask;
               int middle = Math.min(this.wlen, endWord);
               if(startWord / 4096 == middle / 4096) {
                  Arrays.fill(this.bits[startWord / 4096], (startWord + 1) % 4096, middle % 4096, 0L);
               } else {
                  while(true) {
                     ++startWord;
                     if(startWord >= middle) {
                        break;
                     }

                     this.bits[startWord / 4096][startWord % 4096] = 0L;
                  }
               }

               if(endWord < this.wlen) {
                  this.bits[endWord / 4096][endWord % 4096] &= endmask;
               }

            }
         }
      }
   }

   public void clear(long startIndex, long endIndex) {
      if(endIndex > startIndex) {
         int startWord = (int)(startIndex >> 6);
         if(startWord < this.wlen) {
            int endWord = (int)(endIndex - 1L >> 6);
            long startmask = -1L << (int)startIndex;
            long endmask = -1L >>> (int)(-endIndex);
            startmask = ~startmask;
            endmask = ~endmask;
            if(startWord == endWord) {
               this.bits[startWord / 4096][startWord % 4096] &= startmask | endmask;
            } else {
               this.bits[startWord / 4096][startWord % 4096] &= startmask;
               int middle = Math.min(this.wlen, endWord);
               if(startWord / 4096 == middle / 4096) {
                  Arrays.fill(this.bits[startWord / 4096], (startWord + 1) % 4096, middle % 4096, 0L);
               } else {
                  while(true) {
                     ++startWord;
                     if(startWord >= middle) {
                        break;
                     }

                     this.bits[startWord / 4096][startWord % 4096] = 0L;
                  }
               }

               if(endWord < this.wlen) {
                  this.bits[endWord / 4096][endWord % 4096] &= endmask;
               }

            }
         }
      }
   }

   public long cardinality() {
      long bitCount = 0L;

      for(int i = this.getPageCount(); i-- > 0; bitCount += BitUtil.pop_array(this.bits[i], 0, this.wlen)) {
         ;
      }

      return bitCount;
   }

   public void intersect(OpenBitSet other) {
      int newLen = Math.min(this.wlen, other.wlen);
      long[][] thisArr = this.bits;
      long[][] otherArr = other.bits;
      int thisPageSize = 4096;
      int otherPageSize = 4096;
      int pos = newLen;

      while(true) {
         --pos;
         if(pos < 0) {
            if(this.wlen > newLen) {
               for(pos = this.wlen; pos-- > newLen; thisArr[pos / thisPageSize][pos % thisPageSize] = 0L) {
                  ;
               }
            }

            this.wlen = newLen;
            return;
         }

         thisArr[pos / thisPageSize][pos % thisPageSize] &= otherArr[pos / otherPageSize][pos % otherPageSize];
      }
   }

   public void and(OpenBitSet other) {
      this.intersect(other);
   }

   public void trimTrailingZeros() {
      int idx;
      for(idx = this.wlen - 1; idx >= 0 && this.bits[idx / 4096][idx % 4096] == 0L; --idx) {
         ;
      }

      this.wlen = idx + 1;
   }

   public static long bits2words(long numBits) {
      return (numBits - 1L >>> 6) + 1L;
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof OpenBitSet)) {
         return false;
      } else {
         OpenBitSet b = (OpenBitSet)o;
         OpenBitSet a;
         if(b.wlen > this.wlen) {
            a = b;
            b = this;
         } else {
            a = this;
         }

         int aPageSize = 4096;
         int bPageSize = 4096;

         int i;
         for(i = a.wlen - 1; i >= b.wlen; --i) {
            if(a.bits[i / aPageSize][i % aPageSize] != 0L) {
               return false;
            }
         }

         for(i = b.wlen - 1; i >= 0; --i) {
            if(a.bits[i / aPageSize][i % aPageSize] != b.bits[i / bPageSize][i % bPageSize]) {
               return false;
            }
         }

         return true;
      }
   }

   public int hashCode() {
      long h = 0L;
      int i = this.wlen;

      while(true) {
         --i;
         if(i < 0) {
            return (int)(h >> 32 ^ h) + -1737092556;
         }

         h ^= this.bits[i / 4096][i % 4096];
         h = h << 1 | h >>> 63;
      }
   }

   public void close() {
   }

   public void serialize(DataOutput out) throws IOException {
      int bitLength = this.getNumWords();
      int pageSize = this.getPageSize();
      int pageCount = this.getPageCount();
      out.writeInt(bitLength);

      for(int p = 0; p < pageCount; ++p) {
         long[] bits = this.getPage(p);

         for(int i = 0; i < pageSize && bitLength-- > 0; ++i) {
            out.writeLong(bits[i]);
         }
      }

   }

   public long serializedSize() {
      int bitLength = this.getNumWords();
      int pageSize = this.getPageSize();
      int pageCount = this.getPageCount();
      long size = (long)TypeSizes.sizeof(bitLength);

      for(int p = 0; p < pageCount; ++p) {
         long[] bits = this.getPage(p);

         for(int i = 0; i < pageSize && bitLength-- > 0; ++i) {
            size += (long)TypeSizes.sizeof(bits[i]);
         }
      }

      return size;
   }

   public void clear() {
      this.clear(0L, this.capacity());
   }

   public static OpenBitSet deserialize(DataInput in) throws IOException {
      long bitLength = (long)in.readInt();
      OpenBitSet bs = new OpenBitSet(bitLength << 6);
      int pageSize = bs.getPageSize();
      int pageCount = bs.getPageCount();

      for(int p = 0; p < pageCount; ++p) {
         long[] bits = bs.getPage(p);

         for(int i = 0; i < pageSize && bitLength-- > 0L; ++i) {
            bits[i] = in.readLong();
         }
      }

      return bs;
   }
}
