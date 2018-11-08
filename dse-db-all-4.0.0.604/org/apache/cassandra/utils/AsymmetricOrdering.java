package org.apache.cassandra.utils;

import com.google.common.collect.Ordering;
import java.util.List;

public abstract class AsymmetricOrdering<T1, T2> extends Ordering<T1> {
   public AsymmetricOrdering() {
   }

   public abstract int compareAsymmetric(T1 var1, T2 var2);

   public int binarySearchAsymmetric(List<? extends T1> searchIn, T2 searchFor, AsymmetricOrdering.Op op) {
      int strictnessOfLessThan = strictnessOfLessThan(op);
      int lb = -1;
      int ub = searchIn.size();

      while(lb + 1 < ub) {
         int m = (lb + ub) / 2;
         int c = this.compareAsymmetric(searchIn.get(m), searchFor);
         if(c < strictnessOfLessThan) {
            lb = m;
         } else {
            ub = m;
         }
      }

      return selectBoundary(op, lb, ub);
   }

   private static int strictnessOfLessThan(AsymmetricOrdering.Op op) {
      switch(null.$SwitchMap$org$apache$cassandra$utils$AsymmetricOrdering$Op[op.ordinal()]) {
      case 1:
      case 2:
         return 1;
      case 3:
      case 4:
         return 0;
      default:
         throw new IllegalStateException();
      }
   }

   private static int selectBoundary(AsymmetricOrdering.Op op, int lb, int ub) {
      switch(null.$SwitchMap$org$apache$cassandra$utils$AsymmetricOrdering$Op[op.ordinal()]) {
      case 1:
      case 4:
         return lb;
      case 2:
      case 3:
         return ub;
      default:
         throw new IllegalStateException();
      }
   }

   public AsymmetricOrdering<T1, T2> reverse() {
      return new AsymmetricOrdering.Reversed(null);
   }

   private class Reversed extends AsymmetricOrdering<T1, T2> {
      private Reversed() {
      }

      public int compareAsymmetric(T1 left, T2 right) {
         return -AsymmetricOrdering.this.compareAsymmetric(left, right);
      }

      public int compare(T1 left, T1 right) {
         return AsymmetricOrdering.this.compare(right, left);
      }

      public AsymmetricOrdering<T1, T2> reverse() {
         return AsymmetricOrdering.this;
      }
   }

   public static enum Op {
      LOWER,
      FLOOR,
      CEIL,
      HIGHER;

      private Op() {
      }
   }
}
