package org.apache.cassandra.dht;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang3.ObjectUtils;

public class Range<T extends RingPosition<T>> extends AbstractBounds<T> implements Comparable<Range<T>>, Serializable {
   public static final long serialVersionUID = 1L;

   public Range(T left, T right) {
      super(left, right);
   }

   public static <T extends RingPosition<T>> boolean contains(T left, T right, T point) {
      return isWrapAround(left, right)?(point.compareTo(left) > 0?true:right.compareTo(point) >= 0):point.compareTo(left) > 0 && right.compareTo(point) >= 0;
   }

   public boolean contains(AbstractBounds<T> that) {
      if(this.left.equals(this.right)) {
         return true;
      } else {
         boolean thiswraps = isWrapAround(this.left, this.right);
         boolean thatwraps = isWrapAround(that.left, that.right);
         return thiswraps == thatwraps?this.left.compareTo(that.left) <= 0 && that.right.compareTo(this.right) <= 0:(!thiswraps?false:this.left.compareTo(that.left) <= 0 || that.right.compareTo(this.right) <= 0);
      }
   }

   public boolean contains(T point) {
      return contains(this.left, this.right, point);
   }

   public boolean intersects(Range<T> that) {
      return this.intersectionWith(that).size() > 0;
   }

   public boolean intersects(AbstractBounds<T> that) {
      if(that instanceof Range) {
         return this.intersects((Range)that);
      } else if(that instanceof Bounds) {
         return this.intersects((Bounds)that);
      } else {
         throw new UnsupportedOperationException("Intersection is only supported for Bounds and Range objects; found " + that.getClass());
      }
   }

   public boolean intersects(Bounds<T> that) {
      return this.contains(that.left) || !that.left.equals(that.right) && this.intersects(new Range(that.left, that.right));
   }

   @SafeVarargs
   public static <T extends RingPosition<T>> Set<Range<T>> rangeSet(Range... ranges) {
      return Collections.unmodifiableSet(new HashSet(Arrays.asList(ranges)));
   }

   public static <T extends RingPosition<T>> Set<Range<T>> rangeSet(Range<T> range) {
      return Collections.singleton(range);
   }

   public Set<Range<T>> intersectionWith(Range<T> that) {
      if(that.contains((AbstractBounds)this)) {
         return rangeSet(this);
      } else if(this.contains((AbstractBounds)that)) {
         return rangeSet(that);
      } else {
         boolean thiswraps = isWrapAround(this.left, this.right);
         boolean thatwraps = isWrapAround(that.left, that.right);
         if(!thiswraps && !thatwraps) {
            return this.left.compareTo(that.right) < 0 && that.left.compareTo(this.right) < 0?rangeSet(new Range((RingPosition)ObjectUtils.max(new RingPosition[]{this.left, that.left}), (RingPosition)ObjectUtils.min(new RingPosition[]{this.right, that.right}))):Collections.emptySet();
         } else if(thiswraps && thatwraps) {
            assert !this.left.equals(that.left);

            return this.left.compareTo(that.left) < 0?intersectionBothWrapping(this, that):intersectionBothWrapping(that, this);
         } else {
            return thiswraps?intersectionOneWrapping(this, that):intersectionOneWrapping(that, this);
         }
      }
   }

   private static <T extends RingPosition<T>> Set<Range<T>> intersectionBothWrapping(Range<T> first, Range<T> that) {
      Set<Range<T>> intersection = new HashSet(2);
      if(that.right.compareTo(first.left) > 0) {
         intersection.add(new Range(first.left, that.right));
      }

      intersection.add(new Range(that.left, first.right));
      return Collections.unmodifiableSet(intersection);
   }

   private static <T extends RingPosition<T>> Set<Range<T>> intersectionOneWrapping(Range<T> wrapping, Range<T> other) {
      Set<Range<T>> intersection = new HashSet(2);
      if(other.contains(wrapping.right)) {
         intersection.add(new Range(other.left, wrapping.right));
      }

      if(other.contains(wrapping.left) && wrapping.left.compareTo(other.right) < 0) {
         intersection.add(new Range(wrapping.left, other.right));
      }

      return Collections.unmodifiableSet(intersection);
   }

   public Pair<AbstractBounds<T>, AbstractBounds<T>> split(T position) {
      assert this.contains(position) || this.left.equals(position);

      if(!position.equals(this.left) && !position.equals(this.right)) {
         AbstractBounds<T> lb = new Range(this.left, position);
         AbstractBounds<T> rb = new Range(position, this.right);
         return Pair.create(lb, rb);
      } else {
         return null;
      }
   }

   public boolean inclusiveLeft() {
      return false;
   }

   public boolean inclusiveRight() {
      return true;
   }

   public ImmutableList<Range<T>> unwrap() {
      T minValue = this.right.minValue();
      return !this.isTrulyWrapAround()?ImmutableList.of(this):ImmutableList.of(new Range(this.left, minValue), new Range(minValue, this.right));
   }

   private static <T extends RingPosition<T>> boolean isWrapAround(T left, T right) {
      return left.compareTo(right) >= 0;
   }

   public int compareTo(Range<T> rhs) {
      boolean lhsWrap = isWrapAround(this.left, this.right);
      boolean rhsWrap = isWrapAround(rhs.left, rhs.right);
      return lhsWrap != rhsWrap?Boolean.compare(!lhsWrap, !rhsWrap):this.right.compareTo(rhs.right);
   }

   private ArrayList<Range<T>> subtractContained(Range<T> contained) {
      ArrayList<Range<T>> difference = new ArrayList(2);
      if(!this.left.equals(contained.left)) {
         difference.add(new Range(this.left, contained.left));
      }

      if(!this.right.equals(contained.right)) {
         difference.add(new Range(contained.right, this.right));
      }

      return difference;
   }

   public Set<Range<T>> subtract(Range<T> rhs) {
      return rhs.differenceToFetch(this);
   }

   public Set<Range<T>> subtractAll(Collection<Range<T>> ranges) {
      Set<Range<T>> result = new HashSet();
      ((Set)result).add(this);

      Range range;
      for(Iterator var3 = ranges.iterator(); var3.hasNext(); result = substractAllFromToken((Set)result, range)) {
         range = (Range)var3.next();
      }

      return (Set)result;
   }

   private static <T extends RingPosition<T>> Set<Range<T>> substractAllFromToken(Set<Range<T>> ranges, Range<T> subtract) {
      Set<Range<T>> result = new HashSet();
      Iterator var3 = ranges.iterator();

      while(var3.hasNext()) {
         Range<T> range = (Range)var3.next();
         result.addAll(range.subtract(subtract));
      }

      return result;
   }

   public Set<Range<T>> differenceToFetch(Range<T> rhs) {
      Set<Range<T>> intersectionSet = this.intersectionWith(rhs);
      HashSet result;
      if(intersectionSet.isEmpty()) {
         result = new HashSet();
         result.add(rhs);
      } else {
         Range<T>[] intersections = new Range[intersectionSet.size()];
         intersectionSet.toArray(intersections);
         if(intersections.length == 1) {
            result = new HashSet(rhs.subtractContained(intersections[0]));
         } else {
            Range<T> first = intersections[0];
            Range<T> second = intersections[1];
            ArrayList<Range<T>> temp = rhs.subtractContained(first);
            Range<T> single = (Range)temp.get(0);
            result = new HashSet(single.subtractContained(second));
         }
      }

      return result;
   }

   public static <T extends RingPosition<T>> boolean isInRanges(T token, Iterable<Range<T>> ranges) {
      assert ranges != null;

      Iterator var2 = ranges.iterator();

      Range range;
      do {
         if(!var2.hasNext()) {
            return false;
         }

         range = (Range)var2.next();
      } while(!range.contains(token));

      return true;
   }

   public boolean equals(Object o) {
      if(!(o instanceof Range)) {
         return false;
      } else {
         Range<?> rhs = (Range)o;
         return this.left.equals(rhs.left) && this.right.equals(rhs.right);
      }
   }

   public String toString() {
      return "(" + this.left + "," + this.right + "]";
   }

   protected String getOpeningString() {
      return "(";
   }

   protected String getClosingString() {
      return "]";
   }

   public boolean isStartInclusive() {
      return false;
   }

   public boolean isEndInclusive() {
      return true;
   }

   public List<String> asList() {
      ArrayList<String> ret = new ArrayList(2);
      ret.add(this.left.toString());
      ret.add(this.right.toString());
      return ret;
   }

   public boolean isWrapAround() {
      return isWrapAround(this.left, this.right);
   }

   public boolean isTrulyWrapAround() {
      return isTrulyWrapAround(this.left, this.right);
   }

   public static <T extends RingPosition<T>> boolean isTrulyWrapAround(T left, T right) {
      T minValue = right.minValue();
      return isWrapAround(left, right) && !right.equals(minValue);
   }

   public static <T extends RingPosition<T>> List<Range<T>> normalize(Collection<Range<T>> ranges) {
      List<Range<T>> output = new ArrayList(ranges.size());
      Iterator var2 = ranges.iterator();

      while(var2.hasNext()) {
         Range<T> range = (Range)var2.next();
         output.addAll(range.unwrap());
      }

      Collections.sort(output, new Comparator<Range<T>>() {
         public int compare(Range<T> b1, Range<T> b2) {
            return b1.left.compareTo(b2.left);
         }
      });
      return deoverlap(output);
   }

   private static <T extends RingPosition<T>> List<Range<T>> deoverlap(List<Range<T>> ranges) {
      if(ranges.isEmpty()) {
         return ranges;
      } else {
         List<Range<T>> output = new ArrayList();
         Iterator<Range<T>> iter = ranges.iterator();
         Range<T> current = (Range)iter.next();
         RingPosition min = current.left.minValue();

         while(true) {
            Range next;
            label36:
            do {
               while(iter.hasNext()) {
                  if(current.right.equals(min)) {
                     if(current.left.equals(min)) {
                        return Collections.singletonList(current);
                     }

                     output.add(new Range(current.left, min));
                     return output;
                  }

                  next = (Range)iter.next();
                  if(next.left.compareTo(current.right) <= 0) {
                     continue label36;
                  }

                  output.add(current);
                  current = next;
               }

               output.add(current);
               return output;
            } while(!next.right.equals(min) && current.right.compareTo(next.right) >= 0);

            current = new Range(current.left, next.right);
         }
      }
   }

   public AbstractBounds<T> withNewRight(T newRight) {
      return new Range(this.left, newRight);
   }

   public static <T extends RingPosition<T>> List<Range<T>> sort(Collection<Range<T>> ranges) {
      List<Range<T>> output = new ArrayList(ranges.size());
      Iterator var2 = ranges.iterator();

      while(var2.hasNext()) {
         Range<T> r = (Range)var2.next();
         output.addAll(r.unwrap());
      }

      Collections.sort(output, new Comparator<Range<T>>() {
         public int compare(Range<T> b1, Range<T> b2) {
            return b1.left.compareTo(b2.left);
         }
      });
      return output;
   }

   public static <T extends RingPosition<T>> List<Range<T>> merge(Collection<Range<T>> ranges) {
      if(ranges.isEmpty()) {
         return Collections.emptyList();
      } else if(ranges.size() == 1) {
         return ImmutableList.copyOf(ranges);
      } else {
         List<Range<T>> sorted = sort(ranges);
         LinkedList<Range<T>> merged = new LinkedList();
         Range<T> first = (Range)sorted.get(0);
         Preconditions.checkState(!first.isTrulyWrapAround(), String.format("Range %s does wrap around.", new Object[]{first}));
         merged.add(first);
         Iterator var4 = sorted.subList(1, sorted.size()).iterator();

         while(var4.hasNext()) {
            Range<T> candidate = (Range)var4.next();
            Preconditions.checkState(!candidate.isTrulyWrapAround(), String.format("Range %s does wrap around.", new Object[]{candidate}));
            Range<T> last = (Range)merged.getLast();
            if(last.intersects(candidate)) {
               merged.removeLast();
               merged.add(new Range(last.left, candidate.right));
            } else if(!last.contains((AbstractBounds)candidate)) {
               merged.add(candidate);
            }
         }

         return merged;
      }
   }

   public static Range<PartitionPosition> makeRowRange(Token left, Token right) {
      return new Range(left.maxKeyBound(), right.maxKeyBound());
   }

   public static Range<PartitionPosition> makeRowRange(Range<Token> tokenBounds) {
      return makeRowRange((Token)tokenBounds.left, (Token)tokenBounds.right);
   }

   public static class OrderedRangeContainmentChecker {
      private final Iterator<Range<Token>> normalizedRangesIterator;
      private Token lastToken = null;
      private Range<Token> currentRange;

      public OrderedRangeContainmentChecker(Collection<Range<Token>> ranges) {
         this.normalizedRangesIterator = Range.normalize(ranges).iterator();

         assert this.normalizedRangesIterator.hasNext();

         this.currentRange = (Range)this.normalizedRangesIterator.next();
      }

      public boolean contains(Token t) {
         assert this.lastToken == null || this.lastToken.compareTo(t) <= 0;

         for(this.lastToken = t; t.compareTo(this.currentRange.left) > 0; this.currentRange = (Range)this.normalizedRangesIterator.next()) {
            if(t.compareTo(this.currentRange.right) <= 0 || ((Token)this.currentRange.right).compareTo(this.currentRange.left) <= 0) {
               return true;
            }

            if(!this.normalizedRangesIterator.hasNext()) {
               return false;
            }
         }

         return false;
      }
   }
}
