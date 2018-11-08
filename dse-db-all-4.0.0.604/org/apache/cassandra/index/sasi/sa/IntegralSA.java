package org.apache.cassandra.index.sasi.sa;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sasi.disk.OnDiskIndexBuilder;
import org.apache.cassandra.index.sasi.disk.TokenTreeBuilder;
import org.apache.cassandra.utils.Pair;

public class IntegralSA extends SA<ByteBuffer> {
   public IntegralSA(AbstractType<?> comparator, OnDiskIndexBuilder.Mode mode) {
      super(comparator, mode);
   }

   public Term<ByteBuffer> getTerm(ByteBuffer termValue, TokenTreeBuilder tokens) {
      return new ByteTerm(this.charCount, termValue, tokens);
   }

   public TermIterator finish() {
      return new IntegralSA.IntegralSuffixIterator();
   }

   private class IntegralSuffixIterator extends TermIterator {
      private final Iterator<Term<ByteBuffer>> termIterator;

      public IntegralSuffixIterator() {
         Collections.sort(IntegralSA.this.terms, new Comparator<Term<?>>() {
            public int compare(Term<?> a, Term<?> b) {
               return a.compareTo(IntegralSA.this.comparator, b);
            }
         });
         this.termIterator = IntegralSA.this.terms.iterator();
      }

      public ByteBuffer minTerm() {
         return ((Term)IntegralSA.this.terms.get(0)).getTerm();
      }

      public ByteBuffer maxTerm() {
         return ((Term)IntegralSA.this.terms.get(IntegralSA.this.terms.size() - 1)).getTerm();
      }

      protected Pair<IndexedTerm, TokenTreeBuilder> computeNext() {
         if(!this.termIterator.hasNext()) {
            return (Pair)this.endOfData();
         } else {
            Term<ByteBuffer> term = (Term)this.termIterator.next();
            return Pair.create(new IndexedTerm(term.getTerm(), false), term.getTokens().finish());
         }
      }
   }
}
