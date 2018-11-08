package org.apache.cassandra.cql3.restrictions;

import java.util.List;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.schema.ColumnMetadata;

final class TermSlice {
   private final Term[] bounds;
   private final boolean[] boundInclusive;

   private TermSlice(Term start, boolean includeStart, Term end, boolean includeEnd) {
      this.bounds = new Term[]{start, end};
      this.boundInclusive = new boolean[]{includeStart, includeEnd};
   }

   public static TermSlice newInstance(Bound bound, boolean include, Term term) {
      return bound.isStart()?new TermSlice(term, include, (Term)null, false):new TermSlice((Term)null, false, term, include);
   }

   public Term bound(Bound bound) {
      return this.bounds[bound.idx];
   }

   public boolean hasBound(Bound b) {
      return this.bounds[b.idx] != null;
   }

   public boolean isInclusive(Bound b) {
      return this.bounds[b.idx] == null || this.boundInclusive[b.idx];
   }

   public TermSlice merge(TermSlice otherSlice) {
      if(this.hasBound(Bound.START)) {
         assert !otherSlice.hasBound(Bound.START);

         return new TermSlice(this.bound(Bound.START), this.isInclusive(Bound.START), otherSlice.bound(Bound.END), otherSlice.isInclusive(Bound.END));
      } else {
         assert !otherSlice.hasBound(Bound.END);

         return new TermSlice(otherSlice.bound(Bound.START), otherSlice.isInclusive(Bound.START), this.bound(Bound.END), this.isInclusive(Bound.END));
      }
   }

   public String toString() {
      return String.format("(%s %s, %s %s)", new Object[]{this.boundInclusive[0]?">=":">", this.bounds[0], this.boundInclusive[1]?"<=":"<", this.bounds[1]});
   }

   public Operator getIndexOperator(Bound b) {
      return b.isStart()?(this.boundInclusive[b.idx]?Operator.GTE:Operator.GT):(this.boundInclusive[b.idx]?Operator.LTE:Operator.LT);
   }

   public boolean isSupportedBy(ColumnMetadata column, Index index) {
      boolean supported = false;
      if(this.hasBound(Bound.START)) {
         supported |= this.isInclusive(Bound.START)?index.supportsExpression(column, Operator.GTE):index.supportsExpression(column, Operator.GT);
      }

      if(this.hasBound(Bound.END)) {
         supported |= this.isInclusive(Bound.END)?index.supportsExpression(column, Operator.LTE):index.supportsExpression(column, Operator.LT);
      }

      return supported;
   }

   public void addFunctionsTo(List<Function> functions) {
      if(this.hasBound(Bound.START)) {
         this.bound(Bound.START).addFunctionsTo(functions);
      }

      if(this.hasBound(Bound.END)) {
         this.bound(Bound.END).addFunctionsTo(functions);
      }

   }
}
