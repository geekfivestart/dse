package org.apache.cassandra.utils.obs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.utils.concurrent.Ref;

public class OffHeapBitSet implements IBitSet {
   private final Memory bytes;

   public OffHeapBitSet(long numBits) {
      long wordCount = OpenBitSet.bits2words(numBits);
      if(wordCount > 2147483647L) {
         throw new UnsupportedOperationException("Bloom filter size is > 16GB, reduce the bloom_filter_fp_chance");
      } else {
         try {
            long byteCount = wordCount * 8L;
            this.bytes = Memory.allocate(byteCount);
         } catch (OutOfMemoryError var7) {
            throw new RuntimeException("Out of native memory occured, You can avoid it by increasing the system ram space or by increasing bloom_filter_fp_chance.");
         }

         this.clear();
      }
   }

   private OffHeapBitSet(Memory bytes) {
      this.bytes = bytes;
   }

   public long capacity() {
      return this.bytes.size() * 8L;
   }

   public long offHeapSize() {
      return this.bytes.size();
   }

   public void addTo(Ref.IdentityCollection identities) {
      identities.add(this.bytes);
   }

   public boolean get(long index) {
      long i = index >> 3;
      long bit = index & 7L;
      int bitmask = 1 << (int)bit;
      return (this.bytes.getByte(i) & bitmask) != 0;
   }

   public void set(long index) {
      long i = index >> 3;
      long bit = index & 7L;
      int bitmask = 1 << (int)bit;
      this.bytes.setByte(i, (byte)(bitmask | this.bytes.getByte(i)));
   }

   public void set(long offset, byte b) {
      this.bytes.setByte(offset, b);
   }

   public void clear(long index) {
      long i = index >> 3;
      long bit = index & 7L;
      int bitmask = 1 << (int)bit;
      int nativeByte = this.bytes.getByte(i) & 255;
      nativeByte &= ~bitmask;
      this.bytes.setByte(i, (byte)nativeByte);
   }

   public void clear() {
      this.bytes.setMemory(0L, this.bytes.size(), (byte)0);
   }

   public void serialize(DataOutput out) throws IOException {
      out.writeInt((int)(this.bytes.size() / 8L));
      long i = 0L;

      while(i < this.bytes.size()) {
         long value = (long)(((this.bytes.getByte(i++) & 255) << 0) + ((this.bytes.getByte(i++) & 255) << 8) + ((this.bytes.getByte(i++) & 255) << 16)) + ((long)(this.bytes.getByte(i++) & 255) << 24) + ((long)(this.bytes.getByte(i++) & 255) << 32) + ((long)(this.bytes.getByte(i++) & 255) << 40) + ((long)(this.bytes.getByte(i++) & 255) << 48) + ((long)this.bytes.getByte(i++) << 56);
         out.writeLong(value);
      }

   }

   public long serializedSize() {
      return (long)TypeSizes.sizeof((int)this.bytes.size()) + this.bytes.size();
   }

   public static OffHeapBitSet deserialize(DataInput in) throws IOException {
      long byteCount = (long)in.readInt() * 8L;
      Memory memory = Memory.allocate(byteCount);
      long i = 0L;

      while(i < byteCount) {
         long v = in.readLong();
         memory.setByte(i++, (byte)((int)(v >>> 0)));
         memory.setByte(i++, (byte)((int)(v >>> 8)));
         memory.setByte(i++, (byte)((int)(v >>> 16)));
         memory.setByte(i++, (byte)((int)(v >>> 24)));
         memory.setByte(i++, (byte)((int)(v >>> 32)));
         memory.setByte(i++, (byte)((int)(v >>> 40)));
         memory.setByte(i++, (byte)((int)(v >>> 48)));
         memory.setByte(i++, (byte)((int)(v >>> 56)));
      }

      return new OffHeapBitSet(memory);
   }

   public void close() {
      this.bytes.free();
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof OffHeapBitSet)) {
         return false;
      } else {
         OffHeapBitSet b = (OffHeapBitSet)o;
         return this.bytes.equals(b.bytes);
      }
   }

   public int hashCode() {
      long h = 0L;

      for(long i = this.bytes.size(); --i >= 0L; h = h << 1 | h >>> 63) {
         h ^= (long)this.bytes.getByte(i);
      }

      return (int)(h >> 32 ^ h) + -1737092556;
   }

   public String toString() {
      return "[OffHeapBitSet]";
   }
}
