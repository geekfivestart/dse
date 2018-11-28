package org.apache.cassandra.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class OverlapIterator<I extends Comparable<? super I>, V> {
   int nextToInclude;
   final List<Interval<I, V>> sortedByMin;
   int nextToExclude;
   final List<Interval<I, V>> sortedByMax;
   final Set<V> overlaps = new HashSet();
   final Set<V> accessible;

   public OverlapIterator(Collection<Interval<I, V>> intervals) {
      this.accessible = Collections.unmodifiableSet(this.overlaps);
      this.sortedByMax = new ArrayList(intervals);
      Collections.sort(this.sortedByMax, Interval.maxOrdering());
      this.sortedByMin = new ArrayList(this.sortedByMax);
      Collections.sort(this.sortedByMin, Interval.minOrdering());
   }

   public void update(I point) {
      while(this.nextToInclude < this.sortedByMin.size() && ((Comparable)((Interval)this.sortedByMin.get(this.nextToInclude)).min).compareTo(point) <= 0) {
         this.overlaps.add((this.sortedByMin.get(this.nextToInclude++)).data);
      }

      while(this.nextToExclude < this.sortedByMax.size() && ((Comparable)((Interval)this.sortedByMax.get(this.nextToExclude)).max).compareTo(point) < 0) {
         this.overlaps.remove((this.sortedByMax.get(this.nextToExclude++)).data);
      }

   }

   public Set<V> overlaps() {
      return this.accessible;
   }
}
