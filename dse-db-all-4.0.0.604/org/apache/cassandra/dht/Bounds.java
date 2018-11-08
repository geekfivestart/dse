package org.apache.cassandra.dht;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.utils.Pair;

public class Bounds<T extends RingPosition<T>> extends AbstractBounds<T> {
   public Bounds(T left, T right) {
      super(left, right);

      assert !strictlyWrapsAround(left, right) : "[" + left + "," + right + "]";

   }

   public boolean contains(T position) {
      return this.left.equals(position) || (this.right.isMinimum() || !this.left.equals(this.right)) && Range.contains(this.left, this.right, position);
   }

   public Pair<AbstractBounds<T>, AbstractBounds<T>> split(T position) {
      assert this.contains(position);

      if(position.equals(this.right)) {
         return null;
      } else {
         AbstractBounds<T> lb = new Bounds(this.left, position);
         AbstractBounds<T> rb = new Range(position, this.right);
         return Pair.create(lb, rb);
      }
   }

   public boolean inclusiveLeft() {
      return true;
   }

   public boolean inclusiveRight() {
      return true;
   }

   public boolean intersects(Bounds<T> that) {
      return this.contains(that.left) || this.contains(that.right) || that.contains(this.left);
   }

   public List<? extends AbstractBounds<T>> unwrap() {
      return Collections.singletonList(this);
   }

   public boolean equals(Object o) {
      if(!(o instanceof Bounds)) {
         return false;
      } else {
         Bounds<?> rhs = (Bounds)o;
         return this.left.equals(rhs.left) && this.right.equals(rhs.right);
      }
   }

   public String toString() {
      return "[" + this.left + "," + this.right + "]";
   }

   protected String getOpeningString() {
      return "[";
   }

   protected String getClosingString() {
      return "]";
   }

   public static <T extends RingPosition<T>> boolean isInBounds(T token, Iterable<Bounds<T>> bounds) {
      assert bounds != null;

      Iterator var2 = bounds.iterator();

      Bounds bound;
      do {
         if(!var2.hasNext()) {
            return false;
         }

         bound = (Bounds)var2.next();
      } while(!bound.contains(token));

      return true;
   }

   public boolean isStartInclusive() {
      return true;
   }

   public boolean isEndInclusive() {
      return true;
   }

   public static Bounds<PartitionPosition> makeRowBounds(Token left, Token right) {
      return new Bounds(left.minKeyBound(), right.maxKeyBound());
   }

   public AbstractBounds<T> withNewRight(T newRight) {
      return new Bounds(this.left, newRight);
   }

   public static <T extends RingPosition<T>> Set<Bounds<T>> getNonOverlappingBounds(Iterable<Bounds<T>> bounds) {
      ArrayList<Bounds<T>> sortedBounds = Lists.newArrayList(bounds);
      Collections.sort(sortedBounds, new Comparator<Bounds<T>>() {
         public int compare(Bounds<T> o1, Bounds<T> o2) {
            return o1.left.compareTo(o2.left);
         }
      });
      Set<Bounds<T>> nonOverlappingBounds = Sets.newHashSet();
      PeekingIterator it = Iterators.peekingIterator(sortedBounds.iterator());

      while(it.hasNext()) {
         Bounds<T> beginBound = (Bounds)it.next();

         Bounds endBound;
         for(endBound = beginBound; it.hasNext() && endBound.right.compareTo(((Bounds)it.peek()).left) >= 0; endBound = (Bounds)it.next()) {
            ;
         }

         nonOverlappingBounds.add(new Bounds(beginBound.left, endBound.right));
      }

      return nonOverlappingBounds;
   }
}
