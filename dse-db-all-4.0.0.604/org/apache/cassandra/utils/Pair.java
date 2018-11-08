package org.apache.cassandra.utils;

import java.util.Objects;

public class Pair<T1, T2> {
   public final T1 left;
   public final T2 right;

   protected Pair(T1 left, T2 right) {
      this.left = left;
      this.right = right;
   }

   public final int hashCode() {
      int hashCode = 31 + (this.left == null?0:this.left.hashCode());
      return 31 * hashCode + (this.right == null?0:this.right.hashCode());
   }

   public final boolean equals(Object o) {
      if(!(o instanceof Pair)) {
         return false;
      } else {
         Pair that = (Pair)o;
         return Objects.equals(this.left, that.left) && Objects.equals(this.right, that.right);
      }
   }

   public String toString() {
      return "(" + this.left + "," + this.right + ")";
   }

   public static <X, Y> Pair<X, Y> create(X x, Y y) {
      return new Pair(x, y);
   }
}
