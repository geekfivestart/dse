package org.apache.cassandra.dht;

import java.util.Collections;
import java.util.List;
import org.apache.cassandra.utils.Pair;

public class IncludingExcludingBounds<T extends RingPosition<T>> extends AbstractBounds<T> {
   public IncludingExcludingBounds(T left, T right) {
      super(left, right);

      assert !strictlyWrapsAround(left, right) && (right.isMinimum() || left.compareTo(right) != 0) : "(" + left + "," + right + ")";
   }

   public boolean contains(T position) {
      return (Range.contains(this.left, this.right, position) || this.left.equals(position)) && !this.right.equals(position);
   }

   public Pair<AbstractBounds<T>, AbstractBounds<T>> split(T position) {
      assert this.contains(position);

      AbstractBounds<T> lb = new Bounds(this.left, position);
      AbstractBounds<T> rb = new ExcludingBounds(position, this.right);
      return Pair.create(lb, rb);
   }

   public boolean inclusiveLeft() {
      return true;
   }

   public boolean inclusiveRight() {
      return false;
   }

   public List<? extends AbstractBounds<T>> unwrap() {
      return Collections.singletonList(this);
   }

   public boolean equals(Object o) {
      if(!(o instanceof IncludingExcludingBounds)) {
         return false;
      } else {
         IncludingExcludingBounds<?> rhs = (IncludingExcludingBounds)o;
         return this.left.equals(rhs.left) && this.right.equals(rhs.right);
      }
   }

   public String toString() {
      return "[" + this.left + "," + this.right + ")";
   }

   protected String getOpeningString() {
      return "[";
   }

   protected String getClosingString() {
      return ")";
   }

   public boolean isStartInclusive() {
      return true;
   }

   public boolean isEndInclusive() {
      return false;
   }

   public AbstractBounds<T> withNewRight(T newRight) {
      return new IncludingExcludingBounds(this.left, newRight);
   }
}
