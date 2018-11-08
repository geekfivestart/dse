package org.apache.cassandra.index.sasi.conf.view;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sasi.SSTableIndex;
import org.apache.cassandra.index.sasi.disk.OnDiskIndexBuilder;
import org.apache.cassandra.index.sasi.plan.Expression;
import org.apache.cassandra.utils.Interval;
import org.apache.cassandra.utils.IntervalTree;

public class RangeTermTree implements TermTree {
   protected final ByteBuffer min;
   protected final ByteBuffer max;
   protected final IntervalTree<RangeTermTree.Term, SSTableIndex, Interval<RangeTermTree.Term, SSTableIndex>> rangeTree;
   protected final AbstractType<?> comparator;

   public RangeTermTree(ByteBuffer min, ByteBuffer max, IntervalTree<RangeTermTree.Term, SSTableIndex, Interval<RangeTermTree.Term, SSTableIndex>> rangeTree, AbstractType<?> comparator) {
      this.min = min;
      this.max = max;
      this.rangeTree = rangeTree;
      this.comparator = comparator;
   }

   public Set<SSTableIndex> search(Expression e) {
      ByteBuffer minTerm = e.lower == null?this.min:e.lower.value;
      ByteBuffer maxTerm = e.upper == null?this.max:e.upper.value;
      return new HashSet(this.rangeTree.search(Interval.create(new RangeTermTree.Term(minTerm, this.comparator), new RangeTermTree.Term(maxTerm, this.comparator), (SSTableIndex)null)));
   }

   public int intervalCount() {
      return this.rangeTree.intervalCount();
   }

   protected static class Term implements Comparable<RangeTermTree.Term> {
      private final ByteBuffer term;
      private final AbstractType<?> comparator;

      public Term(ByteBuffer term, AbstractType<?> comparator) {
         this.term = term;
         this.comparator = comparator;
      }

      public int compareTo(RangeTermTree.Term o) {
         return this.comparator.compare(this.term, o.term);
      }
   }

   static class Builder extends TermTree.Builder {
      protected final List<Interval<RangeTermTree.Term, SSTableIndex>> intervals = new ArrayList();

      protected Builder(OnDiskIndexBuilder.Mode mode, AbstractType<?> comparator) {
         super(mode, comparator);
      }

      public void addIndex(SSTableIndex index) {
         this.intervals.add(Interval.create(new RangeTermTree.Term(index.minTerm(), this.comparator), new RangeTermTree.Term(index.maxTerm(), this.comparator), index));
      }

      public TermTree build() {
         return new RangeTermTree(this.min, this.max, IntervalTree.build(this.intervals), this.comparator);
      }
   }
}
