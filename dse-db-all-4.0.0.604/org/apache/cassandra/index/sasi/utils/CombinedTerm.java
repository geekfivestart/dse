package org.apache.cassandra.index.sasi.utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sasi.disk.OnDiskIndex;
import org.apache.cassandra.index.sasi.disk.StaticTokenTreeBuilder;
import org.apache.cassandra.index.sasi.disk.Token;
import org.apache.cassandra.index.sasi.disk.TokenTreeBuilder;

public class CombinedTerm implements CombinedValue<OnDiskIndex.DataTerm> {
   private final AbstractType<?> comparator;
   private final OnDiskIndex.DataTerm term;
   private final List<OnDiskIndex.DataTerm> mergedTerms = new ArrayList();

   public CombinedTerm(AbstractType<?> comparator, OnDiskIndex.DataTerm term) {
      this.comparator = comparator;
      this.term = term;
   }

   public ByteBuffer getTerm() {
      return this.term.getTerm();
   }

   public boolean isPartial() {
      return this.term.isPartial();
   }

   public RangeIterator<Long, Token> getTokenIterator() {
      RangeIterator.Builder<Long, Token> union = RangeUnionIterator.builder();
      union.add(this.term.getTokens());
      this.mergedTerms.stream().map(OnDiskIndex.DataTerm::getTokens).forEach(union::add);
      return union.build();
   }

   public TokenTreeBuilder getTokenTreeBuilder() {
      return (new StaticTokenTreeBuilder(this)).finish();
   }

   public void merge(CombinedValue<OnDiskIndex.DataTerm> other) {
      if(other instanceof CombinedTerm) {
         CombinedTerm o = (CombinedTerm)other;

         assert this.comparator == o.comparator;

         this.mergedTerms.add(o.term);
      }
   }

   public OnDiskIndex.DataTerm get() {
      return this.term;
   }

   public int compareTo(CombinedValue<OnDiskIndex.DataTerm> o) {
      return this.term.compareTo(this.comparator, ((OnDiskIndex.DataTerm)o.get()).getTerm());
   }
}
