package org.apache.cassandra.index.sasi.disk;

import com.carrotsearch.hppc.LongSet;
import com.google.common.collect.Iterators;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import org.apache.cassandra.index.sasi.utils.CombinedTerm;
import org.apache.cassandra.index.sasi.utils.RangeIterator;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.Pair;

public class StaticTokenTreeBuilder extends AbstractTokenTreeBuilder {
   private final CombinedTerm combinedTerm;

   public StaticTokenTreeBuilder(CombinedTerm term) {
      this.combinedTerm = term;
   }

   public void add(Long token, long keyPosition) {
      throw new UnsupportedOperationException();
   }

   public void add(SortedMap<Long, LongSet> data) {
      throw new UnsupportedOperationException();
   }

   public void add(Iterator<Pair<Long, LongSet>> data) {
      throw new UnsupportedOperationException();
   }

   public boolean isEmpty() {
      return this.tokenCount == 0L;
   }

   public Iterator<Pair<Long, LongSet>> iterator() {
      final Iterator<Token> iterator = this.combinedTerm.getTokenIterator();
      return new AbstractIterator<Pair<Long, LongSet>>() {
         protected Pair<Long, LongSet> computeNext() {
            if(!iterator.hasNext()) {
               return (Pair)this.endOfData();
            } else {
               Token token = (Token)iterator.next();
               return Pair.create(token.get(), token.getOffsets());
            }
         }
      };
   }

   public long getTokenCount() {
      return this.tokenCount;
   }

   public void write(DataOutputPlus out) throws IOException {
      super.write(out);
      if(!this.root.isLeaf()) {
         RangeIterator<Long, Token> tokens = this.combinedTerm.getTokenIterator();
         ByteBuffer blockBuffer = ByteBuffer.allocate(4096);
         Iterator leafIterator = this.leftmostLeaf.levelIterator();

         while(leafIterator.hasNext()) {
            AbstractTokenTreeBuilder.Leaf leaf = (AbstractTokenTreeBuilder.Leaf)leafIterator.next();
            AbstractTokenTreeBuilder.Leaf writeableLeaf = new StaticTokenTreeBuilder.StaticLeaf(Iterators.limit(tokens, leaf.tokenCount()), leaf);
            writeableLeaf.serialize(-1L, blockBuffer);
            this.flushBuffer(blockBuffer, out, true);
         }

      }
   }

   protected void constructTree() {
      RangeIterator<Long, Token> tokens = this.combinedTerm.getTokenIterator();
      this.tokenCount = 0L;
      this.treeMinToken = ((Long)tokens.getMinimum()).longValue();
      this.treeMaxToken = ((Long)tokens.getMaximum()).longValue();
      this.numBlocks = 1;
      this.root = new AbstractTokenTreeBuilder.InteriorNode();
      this.rightmostParent = (AbstractTokenTreeBuilder.InteriorNode)this.root;
      AbstractTokenTreeBuilder.Leaf lastLeaf = null;
      Long firstToken = null;
      int leafSize = 0;

      while(true) {
         Long token;
         do {
            if(!tokens.hasNext()) {
               if(this.root.tokenCount() == 0) {
                  this.numBlocks = 1;
                  this.root = new StaticTokenTreeBuilder.StaticLeaf(this.combinedTerm.getTokenIterator(), Long.valueOf(this.treeMinToken), Long.valueOf(this.treeMaxToken), this.tokenCount, true);
               }

               return;
            }

            token = ((Token)tokens.next()).get();
            if(firstToken == null) {
               firstToken = token;
            }

            ++this.tokenCount;
            ++leafSize;
         } while(this.tokenCount % 248L != 0L && token.longValue() != this.treeMaxToken);

         AbstractTokenTreeBuilder.Leaf leaf = new StaticTokenTreeBuilder.PartialLeaf(firstToken, token, leafSize);
         if(lastLeaf == null) {
            this.leftmostLeaf = leaf;
         } else {
            lastLeaf.next = leaf;
         }

         this.rightmostParent.add(leaf);
         lastLeaf = this.rightmostLeaf = leaf;
         firstToken = null;
         ++this.numBlocks;
         leafSize = 0;
      }
   }

   private class StaticLeaf extends AbstractTokenTreeBuilder.Leaf {
      private final Iterator<Token> tokens;
      private final int count;
      private final boolean isLast;

      public StaticLeaf(Iterator<Token> var1, AbstractTokenTreeBuilder.Leaf tokens) {
         this(tokens, leaf.smallestToken(), leaf.largestToken(), (long)leaf.tokenCount(), leaf.isLastLeaf());
      }

      public StaticLeaf(Iterator<Token> var1, Long tokens, Long min, long max, boolean count) {
         super(min, max);
         this.count = (int)count;
         this.tokens = tokens;
         this.isLast = isLastLeaf;
      }

      public boolean isLastLeaf() {
         return this.isLast;
      }

      public int tokenCount() {
         return this.count;
      }

      public void serializeData(ByteBuffer buf) {
         while(this.tokens.hasNext()) {
            Token entry = (Token)this.tokens.next();
            this.createEntry(entry.get().longValue(), entry.getOffsets()).serialize(buf);
         }

      }

      public boolean isSerializable() {
         return true;
      }
   }

   private class PartialLeaf extends AbstractTokenTreeBuilder.Leaf {
      private final int size;

      public PartialLeaf(Long min, Long max, int count) {
         super(min, max);
         this.size = count;
      }

      public int tokenCount() {
         return this.size;
      }

      public void serializeData(ByteBuffer buf) {
         throw new UnsupportedOperationException();
      }

      public boolean isSerializable() {
         return false;
      }
   }
}
