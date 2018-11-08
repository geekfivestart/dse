package org.apache.cassandra.io.tries;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.cassandra.utils.SizedInts;

public abstract class TrieNode {
   final int bytesPerPointer;
   static final int FRACTIONAL_BYTES = 0;
   int ordinal = -1;
   static final TrieNode.PayloadOnly PAYLOAD_ONLY = new TrieNode.PayloadOnly();
   static final TrieNode.Single SINGLE_8 = new TrieNode.Single(1);
   static final TrieNode.Single SINGLE_16 = new TrieNode.Single(2);
   static final TrieNode.Single SINGLE_NOPAYLOAD_4 = new TrieNode.SingleNoPayload4();
   static final TrieNode.Single SINGLE_NOPAYLOAD_12 = new TrieNode.SingleNoPayload12();
   static final TrieNode.Sparse SPARSE_8 = new TrieNode.Sparse(1);
   static final TrieNode.Sparse SPARSE_16 = new TrieNode.Sparse(2);
   static final TrieNode.Sparse SPARSE_24 = new TrieNode.Sparse(3);
   static final TrieNode.Sparse SPARSE_40 = new TrieNode.Sparse(5);
   static final TrieNode.Sparse12 SPARSE_12 = new TrieNode.Sparse12();
   static final TrieNode.Dense DENSE_16 = new TrieNode.Dense(2);
   static final TrieNode.Dense DENSE_24 = new TrieNode.Dense(3);
   static final TrieNode.Dense DENSE_32 = new TrieNode.Dense(4);
   static final TrieNode.Dense DENSE_40 = new TrieNode.Dense(5);
   static final TrieNode.Dense12 DENSE_12 = new TrieNode.Dense12();
   static final TrieNode.LongDense LONG_DENSE = new TrieNode.LongDense();
   static final TrieNode[] values;
   static final TrieNode[] singles;
   static final TrieNode[] sparses;
   static final TrieNode[] denses;
   public static final ByteBuffer EMPTY;

   public static TrieNode at(ByteBuffer src, int position) {
      return values[src.get(position) >> 4 & 15];
   }

   public int payloadFlags(ByteBuffer src, int position) {
      return src.get(position) & 15;
   }

   public abstract int payloadPosition(ByteBuffer var1, int var2);

   public abstract int search(ByteBuffer var1, int var2, int var3);

   public abstract int transitionByte(ByteBuffer var1, int var2, int var3);

   public abstract int transitionRange(ByteBuffer var1, int var2);

   abstract long transitionDelta(ByteBuffer var1, int var2, int var3);

   public long transition(ByteBuffer src, int position, long positionLong, int searchIndex) {
      return positionLong + this.transitionDelta(src, position, searchIndex);
   }

   public long lastTransition(ByteBuffer src, int position, long positionLong) {
      return this.transition(src, position, positionLong, this.transitionRange(src, position) - 1);
   }

   public abstract long greaterTransition(ByteBuffer var1, int var2, long var3, int var5, long var6);

   public abstract long lesserTransition(ByteBuffer var1, int var2, long var3, int var5);

   public static TrieNode typeFor(SerializationNode<?> node, long nodePosition) {
      int c = node.childCount();
      if(c == 0) {
         return PAYLOAD_ONLY;
      } else {
         int bitsPerPointerIndex = 0;
         long delta = node.maxPositionDelta(nodePosition);

         assert delta < 0L;

         while(!singles[bitsPerPointerIndex].fits(-delta)) {
            ++bitsPerPointerIndex;
         }

         if(c == 1) {
            if(node.payload() != null && singles[bitsPerPointerIndex].bytesPerPointer == 0) {
               ++bitsPerPointerIndex;
            }

            return singles[bitsPerPointerIndex];
         } else {
            TrieNode sparse = sparses[bitsPerPointerIndex];
            TrieNode dense = denses[bitsPerPointerIndex];
            return sparse.sizeofNode(node) < dense.sizeofNode(node)?sparse:dense;
         }
      }
   }

   public abstract int sizeofNode(SerializationNode<?> var1);

   public abstract void serialize(DataOutput var1, SerializationNode<?> var2, int var3, long var4) throws IOException;

   TrieNode(int bytesPerPointer) {
      this.bytesPerPointer = bytesPerPointer;
   }

   static int read12Bits(ByteBuffer src, int base, int searchIndex) {
      int word = src.getShort(base + 3 * searchIndex / 2);
      if((searchIndex & 1) == 0) {
         word >>= 4;
      }

      return word & 4095;
   }

   static int write12Bits(DataOutput dest, int value, int index, int carry) throws IOException {
      assert 0 <= value && value <= 4095;

      if((index & 1) == 0) {
         dest.writeByte(value >> 4);
         return value << 4;
      } else {
         dest.writeByte(carry | value >> 8);
         dest.writeByte(value);
         return 0;
      }
   }

   long readBytes(ByteBuffer src, int position) {
      return SizedInts.readUnsigned(src, position, this.bytesPerPointer);
   }

   void writeBytes(DataOutput dest, long ofs) throws IOException {
      assert this.fits(ofs);

      SizedInts.write(dest, ofs, this.bytesPerPointer);
   }

   boolean fits(long delta) {
      return 0L <= delta && delta < 1L << this.bytesPerPointer * 8;
   }

   public String toString() {
      String res = this.getClass().getSimpleName();
      if(this.bytesPerPointer >= 1) {
         res = res + this.bytesPerPointer * 8;
      }

      return res;
   }

   public static Object nodeTypeString(int ordinal) {
      return values[ordinal].toString();
   }

   static {
      values = new TrieNode[]{PAYLOAD_ONLY, SINGLE_NOPAYLOAD_4, SINGLE_8, SINGLE_NOPAYLOAD_12, SINGLE_16, SPARSE_8, SPARSE_12, SPARSE_16, SPARSE_24, SPARSE_40, DENSE_12, DENSE_16, DENSE_24, DENSE_32, DENSE_40, LONG_DENSE};
      singles = new TrieNode[]{SINGLE_NOPAYLOAD_4, SINGLE_8, SINGLE_NOPAYLOAD_12, SINGLE_16, DENSE_24, DENSE_32, DENSE_40, LONG_DENSE};
      sparses = new TrieNode[]{SPARSE_8, SPARSE_8, SPARSE_12, SPARSE_16, SPARSE_24, SPARSE_40, SPARSE_40, LONG_DENSE};
      denses = new TrieNode[]{DENSE_12, DENSE_12, DENSE_12, DENSE_16, DENSE_24, DENSE_32, DENSE_40, LONG_DENSE};

      assert sparses.length == singles.length;

      assert denses.length == singles.length;

      assert values.length <= 16;

      for(int i = 0; i < values.length; values[i].ordinal = i++) {
         ;
      }

      EMPTY = ByteBuffer.wrap(new byte[]{(byte)(PAYLOAD_ONLY.ordinal << 4)});
   }

   static class LongDense extends TrieNode.Dense {
      LongDense() {
         super(8);
      }

      public long transitionDelta(ByteBuffer src, int position, int childIndex) {
         return -src.getLong(position + 3 + childIndex * 8);
      }

      public void writeBytes(DataOutput dest, long ofs) throws IOException {
         dest.writeLong(ofs);
      }

      boolean fits(long delta) {
         return true;
      }
   }

   static class Dense12 extends TrieNode.Dense {
      Dense12() {
         super(0);
      }

      public int payloadPosition(ByteBuffer src, int position) {
         return position + 3 + (this.transitionRange(src, position) * 3 + 1) / 2;
      }

      public long transitionDelta(ByteBuffer src, int position, int searchIndex) {
         return (long)(-read12Bits(src, position + 3, searchIndex));
      }

      public int sizeofNode(SerializationNode<?> node) {
         int l = node.transition(0);
         int r = node.transition(node.childCount() - 1);
         return 3 + ((r - l + 1) * 3 + 1) / 2;
      }

      public void serialize(DataOutput dest, SerializationNode<?> node, int payloadBits, long nodePosition) throws IOException {
         int childCount = node.childCount();
         dest.writeByte((this.ordinal << 4) + (payloadBits & 15));
         int l = node.transition(0);
         int r = node.transition(childCount - 1);

         assert 0 <= l && l <= r && r <= 255;

         dest.writeByte(l);
         dest.writeByte(r - l);
         int carry = 0;
         int start = l;

         for(int i = 0; i < childCount; ++i) {
            for(int next = node.transition(i); l < next; ++l) {
               carry = write12Bits(dest, 0, l - start, carry);
            }

            long pd = node.serializedPositionDelta(i, nodePosition);
            carry = write12Bits(dest, (int)(-pd), l - start, carry);
            ++l;
         }

         if((l - start & 1) == 1) {
            dest.writeByte(carry);
         }

      }

      boolean fits(long delta) {
         return 0L <= delta && delta <= 4095L;
      }
   }

   static class Dense extends TrieNode {
      static final int NULL_VALUE = 0;

      Dense(int bytesPerPointer) {
         super(bytesPerPointer);
      }

      public int transitionRange(ByteBuffer src, int position) {
         return 1 + (src.get(position + 2) & 255);
      }

      public int payloadPosition(ByteBuffer src, int position) {
         return position + 3 + this.transitionRange(src, position) * this.bytesPerPointer;
      }

      public int search(ByteBuffer src, int position, int transitionByte) {
         int l = src.get(position + 1) & 255;
         int i = transitionByte - l;
         if(i < 0) {
            return -1;
         } else {
            int len = this.transitionRange(src, position);
            if(i >= len) {
               return -len - 1;
            } else {
               long t = this.transition(src, position, 0L, i);
               return t != -1L?i:-i - 1;
            }
         }
      }

      public long transitionDelta(ByteBuffer src, int position, int searchIndex) {
         return -this.readBytes(src, position + 3 + searchIndex * this.bytesPerPointer);
      }

      public long transition(ByteBuffer src, int position, long positionLong, int searchIndex) {
         long v = this.transitionDelta(src, position, searchIndex);
         return v != 0L?v + positionLong:-1L;
      }

      public long greaterTransition(ByteBuffer src, int position, long positionLong, int searchIndex, long defaultValue) {
         if(searchIndex < 0) {
            searchIndex = -1 - searchIndex;
         } else {
            ++searchIndex;
         }

         for(int len = this.transitionRange(src, position); searchIndex < len; ++searchIndex) {
            long t = this.transition(src, position, positionLong, searchIndex);
            if(t != -1L) {
               return t;
            }
         }

         return defaultValue;
      }

      public long lesserTransition(ByteBuffer src, int position, long positionLong, int searchIndex) {
         if(searchIndex < 0) {
            searchIndex = -2 - searchIndex;
         } else {
            --searchIndex;
         }

         while(searchIndex >= 0) {
            long t = this.transition(src, position, positionLong, searchIndex);
            if(t != -1L) {
               return t;
            }

            --searchIndex;
         }

         assert false : "transition must always exist at 0 and we should not be called for less of that";

         return -1L;
      }

      public int transitionByte(ByteBuffer src, int position, int childIndex) {
         if(childIndex >= this.transitionRange(src, position)) {
            return 2147483647;
         } else {
            int l = src.get(position + 1) & 255;
            return l + childIndex;
         }
      }

      public int sizeofNode(SerializationNode<?> node) {
         int l = node.transition(0);
         int r = node.transition(node.childCount() - 1);
         return 3 + (r - l + 1) * this.bytesPerPointer;
      }

      public void serialize(DataOutput dest, SerializationNode<?> node, int payloadBits, long nodePosition) throws IOException {
         int childCount = node.childCount();
         dest.writeByte((this.ordinal << 4) + (payloadBits & 15));
         int l = node.transition(0);
         int r = node.transition(childCount - 1);

         assert 0 <= l && l <= r && r <= 255;

         dest.writeByte(l);
         dest.writeByte(r - l);

         for(int i = 0; i < childCount; ++i) {
            for(int next = node.transition(i); l < next; ++l) {
               this.writeBytes(dest, 0L);
            }

            this.writeBytes(dest, -node.serializedPositionDelta(i, nodePosition));
            ++l;
         }

      }
   }

   static class Sparse12 extends TrieNode.Sparse {
      Sparse12() {
         super(0);
      }

      public int payloadPosition(ByteBuffer src, int position) {
         int count = this.transitionRange(src, position);
         return position + 2 + (5 * count + 1) / 2;
      }

      public long transitionDelta(ByteBuffer src, int position, int searchIndex) {
         return (long)(-read12Bits(src, position + 2 + this.transitionRange(src, position), searchIndex));
      }

      public int sizeofNode(SerializationNode<?> node) {
         return 2 + (node.childCount() * 5 + 1) / 2;
      }

      public void serialize(DataOutput dest, SerializationNode<?> node, int payloadBits, long nodePosition) throws IOException {
         int childCount = node.childCount();

         assert childCount < 256;

         dest.writeByte((this.ordinal << 4) + (payloadBits & 15));
         dest.writeByte(childCount);

         int i;
         for(i = 0; i < childCount; ++i) {
            dest.writeByte(node.transition(i));
         }

         for(i = 0; i + 2 <= childCount; i += 2) {
            int p0 = (int)(-node.serializedPositionDelta(i, nodePosition));
            int p1 = (int)(-node.serializedPositionDelta(i + 1, nodePosition));

            assert p0 > 0 && p0 < 4096;

            assert p1 > 0 && p1 < 4096;

            dest.writeByte(p0 >> 4);
            dest.writeByte(p0 << 4 | p1 >> 8);
            dest.writeByte(p1);
         }

         if(i < childCount) {
            long pd = -node.serializedPositionDelta(i, nodePosition);

            assert pd > 0L && pd < 4096L;

            dest.writeShort((short)((int)(pd << 4)));
         }

      }

      boolean fits(long delta) {
         return 0L <= delta && delta <= 4095L;
      }
   }

   static class Sparse extends TrieNode {
      Sparse(int bytesPerPointer) {
         super(bytesPerPointer);
      }

      public int transitionRange(ByteBuffer src, int position) {
         return src.get(position + 1) & 255;
      }

      public int payloadPosition(ByteBuffer src, int position) {
         int count = this.transitionRange(src, position);
         return position + 2 + (this.bytesPerPointer + 1) * count;
      }

      public int search(ByteBuffer src, int position, int key) {
         int l = -1;
         int r = this.transitionRange(src, position);
         position += 2;

         while(l + 1 < r) {
            int m = (l + r + 1) / 2;
            int childTransition = src.get(position + m) & 255;
            int cmp = Integer.compare(key, childTransition);
            if(cmp < 0) {
               r = m;
            } else {
               if(cmp <= 0) {
                  return m;
               }

               l = m;
            }
         }

         return -r - 1;
      }

      public long transitionDelta(ByteBuffer src, int position, int searchIndex) {
         return -this.readBytes(src, position + 2 + this.transitionRange(src, position) + this.bytesPerPointer * searchIndex);
      }

      public long greaterTransition(ByteBuffer src, int position, long positionLong, int searchIndex, long defaultValue) {
         if(searchIndex < 0) {
            searchIndex = -1 - searchIndex;
         } else {
            ++searchIndex;
         }

         return searchIndex >= this.transitionRange(src, position)?defaultValue:this.transition(src, position, positionLong, searchIndex);
      }

      public long lesserTransition(ByteBuffer src, int position, long positionLong, int searchIndex) {
         if(searchIndex < 0) {
            searchIndex = -2 - searchIndex;
         } else {
            --searchIndex;
         }

         return this.transition(src, position, positionLong, searchIndex);
      }

      public int transitionByte(ByteBuffer src, int position, int childIndex) {
         return childIndex < this.transitionRange(src, position)?src.get(position + 2 + childIndex) & 255:2147483647;
      }

      public int sizeofNode(SerializationNode<?> node) {
         return 2 + node.childCount() * (1 + this.bytesPerPointer);
      }

      public void serialize(DataOutput dest, SerializationNode<?> node, int payloadBits, long nodePosition) throws IOException {
         int childCount = node.childCount();

         assert childCount > 0;

         assert childCount < 256;

         dest.writeByte((this.ordinal << 4) + (payloadBits & 15));
         dest.writeByte(childCount);

         int i;
         for(i = 0; i < childCount; ++i) {
            dest.writeByte(node.transition(i));
         }

         for(i = 0; i < childCount; ++i) {
            this.writeBytes(dest, -node.serializedPositionDelta(i, nodePosition));
         }

      }
   }

   static class SingleNoPayload12 extends TrieNode.Single {
      SingleNoPayload12() {
         super(0);
      }

      public int payloadFlags(ByteBuffer src, int position) {
         return 0;
      }

      public int payloadPosition(ByteBuffer src, int position) {
         return position + 3;
      }

      public int search(ByteBuffer src, int position, int transitionByte) {
         int c = src.get(position + 2) & 255;
         return transitionByte == c?0:(transitionByte < c?-1:-2);
      }

      public long transitionDelta(ByteBuffer src, int position, int searchIndex) {
         return (long)(-(src.getShort(position) & 4095));
      }

      public int transitionByte(ByteBuffer src, int position, int childIndex) {
         return childIndex == 0?src.get(position + 2) & 255:2147483647;
      }

      boolean fits(long delta) {
         return 0L <= delta && delta <= 4095L;
      }

      public void serialize(DataOutput dest, SerializationNode<?> node, int payloadBits, long nodePosition) throws IOException {
         assert payloadBits == 0;

         int childCount = node.childCount();

         assert childCount == 1;

         long pd = -node.serializedPositionDelta(0, nodePosition);

         assert pd > 0L && pd < 4096L;

         dest.writeByte((this.ordinal << 4) + (int)(pd >> 8 & 15L));
         dest.writeByte((byte)((int)pd));
         dest.writeByte(node.transition(0));
      }

      public int sizeofNode(SerializationNode<?> node) {
         return 3;
      }
   }

   static class SingleNoPayload4 extends TrieNode.Single {
      SingleNoPayload4() {
         super(0);
      }

      public int payloadFlags(ByteBuffer src, int position) {
         return 0;
      }

      public int payloadPosition(ByteBuffer src, int position) {
         return position + 2;
      }

      public long transitionDelta(ByteBuffer src, int position, int searchIndex) {
         return (long)(-(src.get(position) & 15));
      }

      boolean fits(long delta) {
         return 0L <= delta && delta <= 15L;
      }

      public void serialize(DataOutput dest, SerializationNode<?> node, int payloadBits, long nodePosition) throws IOException {
         assert payloadBits == 0;

         int childCount = node.childCount();

         assert childCount == 1;

         long pd = -node.serializedPositionDelta(0, nodePosition);

         assert pd > 0L && pd < 16L;

         dest.writeByte((this.ordinal << 4) + (int)(pd & 15L));
         dest.writeByte(node.transition(0));
      }

      public int sizeofNode(SerializationNode<?> node) {
         return 2;
      }
   }

   static class Single extends TrieNode {
      Single(int bytesPerPointer) {
         super(bytesPerPointer);
      }

      public int payloadPosition(ByteBuffer src, int position) {
         return position + 2 + this.bytesPerPointer;
      }

      public int search(ByteBuffer src, int position, int transitionByte) {
         int c = src.get(position + 1) & 255;
         return transitionByte == c?0:(transitionByte < c?-1:-2);
      }

      public long transitionDelta(ByteBuffer src, int position, int searchIndex) {
         return -this.readBytes(src, position + 2);
      }

      public long lastTransition(ByteBuffer src, int position, long positionLong) {
         return this.transition(src, position, positionLong, 0);
      }

      public long greaterTransition(ByteBuffer src, int position, long positionLong, int searchIndex, long defaultValue) {
         return searchIndex == -1?this.transition(src, position, positionLong, 0):defaultValue;
      }

      public long lesserTransition(ByteBuffer src, int position, long positionLong, int searchIndex) {
         return this.transition(src, position, positionLong, 0);
      }

      public int transitionByte(ByteBuffer src, int position, int childIndex) {
         return childIndex == 0?src.get(position + 1) & 255:2147483647;
      }

      public int transitionRange(ByteBuffer src, int position) {
         return 1;
      }

      public int sizeofNode(SerializationNode<?> node) {
         return 2 + this.bytesPerPointer;
      }

      public void serialize(DataOutput dest, SerializationNode<?> node, int payloadBits, long nodePosition) throws IOException {
         int childCount = node.childCount();

         assert childCount == 1;

         dest.writeByte((this.ordinal << 4) + (payloadBits & 15));
         dest.writeByte(node.transition(0));
         this.writeBytes(dest, -node.serializedPositionDelta(0, nodePosition));
      }
   }

   static class PayloadOnly extends TrieNode {
      PayloadOnly() {
         super(0);
      }

      public int payloadPosition(ByteBuffer src, int position) {
         return position + 1;
      }

      public int search(ByteBuffer src, int position, int transitionByte) {
         return -1;
      }

      public long transitionDelta(ByteBuffer src, int position, int searchIndex) {
         return 0L;
      }

      public long transition(ByteBuffer src, int position, long positionLong, int searchIndex) {
         return -1L;
      }

      public long lastTransition(ByteBuffer src, int position, long positionLong) {
         return -1L;
      }

      public long greaterTransition(ByteBuffer src, int position, long positionLong, int searchIndex, long defaultValue) {
         return defaultValue;
      }

      public long lesserTransition(ByteBuffer src, int position, long positionLong, int searchIndex) {
         return -1L;
      }

      public int transitionByte(ByteBuffer src, int position, int childIndex) {
         return 2147483647;
      }

      public int transitionRange(ByteBuffer src, int position) {
         return 0;
      }

      public int sizeofNode(SerializationNode<?> node) {
         return 1;
      }

      public void serialize(DataOutput dest, SerializationNode<?> node, int payloadBits, long nodePosition) throws IOException {
         dest.writeByte((this.ordinal << 4) + (payloadBits & 15));
      }
   }
}
