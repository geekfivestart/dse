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
       private static int strictnessOfLessThan(Op op) {
        switch (op) {
            case FLOOR:
            case HIGHER: {
                return 1;
            }
            case CEIL:
            case LOWER: {
                return 0;
            }
        }
        throw new IllegalStateException();
    }

    private static int selectBoundary(Op op, int lb, int ub) {
        switch (op) {
            case HIGHER:
            case CEIL: {
                return ub;
            }
            case FLOOR:
            case LOWER: {
                return lb;
            }
        }
        throw new IllegalStateException();
    }

   public AsymmetricOrdering<T1, T2> reverse() {
      return new AsymmetricOrdering.Reversed();
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
