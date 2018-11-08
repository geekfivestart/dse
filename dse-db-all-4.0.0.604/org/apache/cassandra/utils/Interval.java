package org.apache.cassandra.utils;

import java.util.Objects;

public class Interval<C, D> {
   public final C min;
   public final C max;
   public final D data;
   private static final AsymmetricOrdering<Interval<Comparable, Object>, Comparable> minOrdering = new AsymmetricOrdering<Interval<Comparable, Object>, Comparable>() {
      public int compareAsymmetric(Interval<Comparable, Object> left, Comparable right) {
         return ((Comparable)left.min).compareTo(right);
      }

      public int compare(Interval<Comparable, Object> i1, Interval<Comparable, Object> i2) {
         return ((Comparable)i1.min).compareTo(i2.min);
      }
   };
   private static final AsymmetricOrdering<Interval<Comparable, Object>, Comparable> maxOrdering = new AsymmetricOrdering<Interval<Comparable, Object>, Comparable>() {
      public int compareAsymmetric(Interval<Comparable, Object> left, Comparable right) {
         return ((Comparable)left.max).compareTo(right);
      }

      public int compare(Interval<Comparable, Object> i1, Interval<Comparable, Object> i2) {
         return ((Comparable)i1.max).compareTo(i2.max);
      }
   };
   private static final AsymmetricOrdering<Interval<Comparable, Object>, Comparable> reverseMaxOrdering;

   public Interval(C min, C max, D data) {
      this.min = min;
      this.max = max;
      this.data = data;
   }

   public static <C, D> Interval<C, D> create(C min, C max) {
      return create(min, max, (Object)null);
   }

   public static <C, D> Interval<C, D> create(C min, C max, D data) {
      return new Interval(min, max, data);
   }

   public String toString() {
      return String.format("[%s, %s]%s", new Object[]{this.min, this.max, this.data == null?"":String.format("(%s)", new Object[]{this.data})});
   }

   public final int hashCode() {
      return Objects.hash(new Object[]{this.min, this.max, this.data});
   }

   public final boolean equals(Object o) {
      if(!(o instanceof Interval)) {
         return false;
      } else {
         Interval that = (Interval)o;
         return Objects.equals(this.min, that.min) && Objects.equals(this.max, that.max) && Objects.equals(this.data, that.data);
      }
   }

   public static <C extends Comparable<? super C>, V> AsymmetricOrdering<Interval<C, V>, C> minOrdering() {
      return minOrdering;
   }

   public static <C extends Comparable<? super C>, V> AsymmetricOrdering<Interval<C, V>, C> maxOrdering() {
      return maxOrdering;
   }

   static {
      reverseMaxOrdering = maxOrdering.reverse();
   }
}
