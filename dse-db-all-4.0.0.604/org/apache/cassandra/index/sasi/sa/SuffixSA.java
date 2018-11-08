package org.apache.cassandra.index.sasi.sa;

import com.google.common.base.Charsets;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sasi.disk.DynamicTokenTreeBuilder;
import org.apache.cassandra.index.sasi.disk.OnDiskIndexBuilder;
import org.apache.cassandra.index.sasi.disk.TokenTreeBuilder;
import org.apache.cassandra.utils.LongTimSort;
import org.apache.cassandra.utils.Pair;

public class SuffixSA extends SA<CharBuffer> {
   public SuffixSA(AbstractType<?> comparator, OnDiskIndexBuilder.Mode mode) {
      super(comparator, mode);
   }

   protected Term<CharBuffer> getTerm(ByteBuffer termValue, TokenTreeBuilder tokens) {
      return new CharTerm(this.charCount, Charsets.UTF_8.decode(termValue.duplicate()), tokens);
   }

   public TermIterator finish() {
      return new SuffixSA.SASuffixIterator();
   }

   private class SASuffixIterator extends TermIterator {
      private static final int COMPLETE_BIT = 31;
      private final long[] suffixes;
      private int current = 0;
      private IndexedTerm lastProcessedSuffix;
      private TokenTreeBuilder container;

      public SASuffixIterator() {
         this.suffixes = new long[SuffixSA.this.charCount];
         long termIndex = -1L;
         long currentTermLength = -1L;
         boolean isComplete = false;

         for(int i = 0; i < SuffixSA.this.charCount; ++i) {
            if((long)i >= currentTermLength || currentTermLength == -1L) {
               Term currentTerm = (Term)SuffixSA.this.terms.get((int)(++termIndex));
               currentTermLength = (long)(currentTerm.getPosition() + currentTerm.length());
               isComplete = true;
            }

            this.suffixes[i] = termIndex << 32 | (long)i;
            if(isComplete) {
               this.suffixes[i] |= 2147483648L;
            }

            isComplete = false;
         }

         LongTimSort.sort(this.suffixes, (a, b) -> {
            Term aTerm = (Term)SuffixSA.this.terms.get((int)(a >>> 32));
            Term bTerm = (Term)SuffixSA.this.terms.get((int)(b >>> 32));
            return SuffixSA.this.comparator.compare(aTerm.getSuffix(this.clearCompleteBit(a) - aTerm.getPosition()), bTerm.getSuffix(this.clearCompleteBit(b) - bTerm.getPosition()));
         });
      }

      private int clearCompleteBit(long value) {
         return (int)(value & -2147483649L);
      }

      private Pair<IndexedTerm, TokenTreeBuilder> suffixAt(int position) {
         long index = this.suffixes[position];
         Term term = (Term)SuffixSA.this.terms.get((int)(index >>> 32));
         boolean isPartitial = (index & 2147483648L) == 0L;
         return Pair.create(new IndexedTerm(term.getSuffix(this.clearCompleteBit(index) - term.getPosition()), isPartitial), term.getTokens());
      }

      public ByteBuffer minTerm() {
         return ((IndexedTerm)this.suffixAt(0).left).getBytes();
      }

      public ByteBuffer maxTerm() {
         return ((IndexedTerm)this.suffixAt(this.suffixes.length - 1).left).getBytes();
      }

      protected Pair<IndexedTerm, TokenTreeBuilder> computeNext() {
         Pair suffix;
         while(this.current < this.suffixes.length) {
            suffix = this.suffixAt(this.current++);
            if(this.lastProcessedSuffix == null) {
               this.lastProcessedSuffix = (IndexedTerm)suffix.left;
               this.container = new DynamicTokenTreeBuilder((TokenTreeBuilder)suffix.right);
            } else {
               if(SuffixSA.this.comparator.compare(this.lastProcessedSuffix.getBytes(), ((IndexedTerm)suffix.left).getBytes()) != 0) {
                  Pair<IndexedTerm, TokenTreeBuilder> result = this.finishSuffix();
                  this.lastProcessedSuffix = (IndexedTerm)suffix.left;
                  this.container = new DynamicTokenTreeBuilder((TokenTreeBuilder)suffix.right);
                  return result;
               }

               this.lastProcessedSuffix = (IndexedTerm)suffix.left;
               this.container.add((TokenTreeBuilder)suffix.right);
            }
         }

         if(this.lastProcessedSuffix == null) {
            return (Pair)this.endOfData();
         } else {
            suffix = this.finishSuffix();
            this.lastProcessedSuffix = null;
            return suffix;
         }
      }

      private Pair<IndexedTerm, TokenTreeBuilder> finishSuffix() {
         return Pair.create(this.lastProcessedSuffix, this.container.finish());
      }
   }
}
