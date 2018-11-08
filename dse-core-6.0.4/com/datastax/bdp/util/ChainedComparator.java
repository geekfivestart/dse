package com.datastax.bdp.util;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class ChainedComparator<T> implements Comparator<T> {
   private final List<Comparator<T>> comparators = new ArrayList();

   public ChainedComparator() {
   }

   public ChainedComparator<T> add(Comparator<T> comparator) {
      Preconditions.checkNotNull(comparator);
      Preconditions.checkArgument(!this.comparators.contains(comparator), "Cannot add duplicate comparator in the chain");
      this.comparators.add(comparator);
      return this;
   }

   public int compare(T left, T right) {
      if(this.comparators.size() == 0) {
         throw new IllegalStateException("ChainedComparator is empty");
      } else {
         Iterator var3 = this.comparators.iterator();

         int result;
         do {
            if(!var3.hasNext()) {
               return 0;
            }

            Comparator<T> comparator = (Comparator)var3.next();
            result = comparator.compare(left, right);
         } while(result == 0);

         return result;
      }
   }

   public int size() {
      return this.comparators.size();
   }
}
