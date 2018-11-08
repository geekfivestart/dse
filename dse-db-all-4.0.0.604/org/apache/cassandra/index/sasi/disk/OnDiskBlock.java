package org.apache.cassandra.index.sasi.disk;

import java.nio.ByteBuffer;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sasi.Term;
import org.apache.cassandra.index.sasi.utils.MappedBuffer;

public abstract class OnDiskBlock<T extends Term> {
   protected final MappedBuffer blockIndex;
   protected final int blockIndexSize;
   protected final boolean hasCombinedIndex;
   protected final TokenTree combinedIndex;

   public OnDiskBlock(Descriptor descriptor, MappedBuffer block, OnDiskBlock.BlockType blockType) {
      this.blockIndex = block;
      if(blockType == OnDiskBlock.BlockType.POINTER) {
         this.hasCombinedIndex = false;
         this.combinedIndex = null;
         this.blockIndexSize = block.getInt() << 1;
      } else {
         long blockOffset = block.position();
         int combinedIndexOffset = block.getInt(blockOffset + 4096L);
         this.hasCombinedIndex = combinedIndexOffset >= 0;
         long blockIndexOffset = blockOffset + 4096L + 4L + (long)combinedIndexOffset;
         this.combinedIndex = this.hasCombinedIndex?new TokenTree(descriptor, this.blockIndex.duplicate().position(blockIndexOffset)):null;
         this.blockIndexSize = block.getInt() * 2;
      }
   }

   public OnDiskBlock.SearchResult<T> search(AbstractType<?> comparator, ByteBuffer query) {
      int cmp = -1;
      int start = 0;
      int end = this.termCount() - 1;
      int middle = 0;
      Term element = null;

      while(start <= end) {
         middle = start + (end - start >> 1);
         element = this.getTerm(middle);
         cmp = element.compareTo(comparator, query);
         if(cmp == 0) {
            return new OnDiskBlock.SearchResult(element, cmp, middle);
         }

         if(cmp < 0) {
            start = middle + 1;
         } else {
            end = middle - 1;
         }
      }

      return new OnDiskBlock.SearchResult(element, cmp, middle);
   }

   protected T getTerm(int index) {
      MappedBuffer dup = this.blockIndex.duplicate();
      long startsAt = this.getTermPosition(index);
      if(this.termCount() - 1 == index) {
         dup.position(startsAt);
      } else {
         dup.position(startsAt).limit(this.getTermPosition(index + 1));
      }

      return this.cast(dup);
   }

   protected long getTermPosition(int idx) {
      return getTermPosition(this.blockIndex, idx, this.blockIndexSize);
   }

   protected int termCount() {
      return this.blockIndexSize >> 1;
   }

   protected abstract T cast(MappedBuffer var1);

   static long getTermPosition(MappedBuffer data, int idx, int indexSize) {
      idx <<= 1;

      assert idx < indexSize;

      return data.position() + (long)indexSize + (long)data.getShort(data.position() + (long)idx);
   }

   public TokenTree getBlockIndex() {
      return this.combinedIndex;
   }

   public int minOffset(OnDiskIndex.IteratorOrder order) {
      return order == OnDiskIndex.IteratorOrder.DESC?0:this.termCount() - 1;
   }

   public int maxOffset(OnDiskIndex.IteratorOrder order) {
      return this.minOffset(order) == 0?this.termCount() - 1:0;
   }

   public static class SearchResult<T> {
      public final T result;
      public final int index;
      public final int cmp;

      public SearchResult(T result, int cmp, int index) {
         this.result = result;
         this.index = index;
         this.cmp = cmp;
      }
   }

   public static enum BlockType {
      POINTER,
      DATA;

      private BlockType() {
      }
   }
}
