package org.apache.cassandra.index.sasi.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.PriorityQueue;
import org.apache.cassandra.io.util.FileUtils;

public class RangeIntersectionIterator {
   public RangeIntersectionIterator() {
   }

   public static <K extends Comparable<K>, D extends CombinedValue<K>> RangeIntersectionIterator.Builder<K, D> builder() {
      return builder(RangeIntersectionIterator.Strategy.ADAPTIVE);
   }

   @VisibleForTesting
   protected static <K extends Comparable<K>, D extends CombinedValue<K>> RangeIntersectionIterator.Builder<K, D> builder(RangeIntersectionIterator.Strategy strategy) {
      return new RangeIntersectionIterator.Builder(strategy);
   }

   @VisibleForTesting
   protected static class LookupIntersectionIterator<K extends Comparable<K>, D extends CombinedValue<K>> extends RangeIntersectionIterator.AbstractIntersectionIterator<K, D> {
      private final RangeIterator<K, D> smallestIterator;

      private LookupIntersectionIterator(RangeIterator.Builder.Statistics<K, D> statistics, PriorityQueue<RangeIterator<K, D>> ranges) {
         super(statistics, ranges, null);
         this.smallestIterator = statistics.minRange;
         if(this.smallestIterator.getCurrent().compareTo(this.getMinimum()) < 0) {
            this.smallestIterator.skipTo(this.getMinimum());
         }

      }

      protected D computeNext() {
         while(this.smallestIterator.hasNext()) {
            D candidate = (CombinedValue)this.smallestIterator.next();
            K token = (Comparable)candidate.get();
            boolean intersectsAll = true;
            Iterator var4 = this.ranges.iterator();

            while(true) {
               if(var4.hasNext()) {
                  RangeIterator<K, D> range = (RangeIterator)var4.next();
                  if(range.equals(this.smallestIterator)) {
                     continue;
                  }

                  if(!isOverlapping(this.smallestIterator, range)) {
                     return (CombinedValue)this.endOfData();
                  }

                  D point = range.skipTo(token);
                  if(point == null) {
                     return (CombinedValue)this.endOfData();
                  }

                  if(((Comparable)point.get()).equals(token)) {
                     candidate.merge(point);
                     continue;
                  }

                  intersectsAll = false;
               }

               if(!intersectsAll) {
                  break;
               }

               return candidate;
            }
         }

         return (CombinedValue)this.endOfData();
      }

      protected void performSkipTo(K nextToken) {
         this.smallestIterator.skipTo(nextToken);
      }
   }

   @VisibleForTesting
   protected static class BounceIntersectionIterator<K extends Comparable<K>, D extends CombinedValue<K>> extends RangeIntersectionIterator.AbstractIntersectionIterator<K, D> {
      private BounceIntersectionIterator(RangeIterator.Builder.Statistics<K, D> statistics, PriorityQueue<RangeIterator<K, D>> ranges) {
         super(statistics, ranges, null);
      }

      protected D computeNext() {
         ArrayList processed = null;

         CombinedValue candidate;
         boolean intersectsAll;
         do {
            if(this.ranges.isEmpty()) {
               return (CombinedValue)this.endOfData();
            }

            RangeIterator<K, D> head = (RangeIterator)this.ranges.poll();
            if(head.getCurrent().compareTo(this.getMinimum()) < 0) {
               head.skipTo(this.getMinimum());
            }

            candidate = head.hasNext()?(CombinedValue)head.next():null;
            if(candidate == null || ((Comparable)candidate.get()).compareTo(this.getMaximum()) > 0) {
               this.ranges.add(head);
               return (CombinedValue)this.endOfData();
            }

            if(processed == null) {
               processed = new ArrayList();
            }

            intersectsAll = true;
            boolean exhausted = false;

            while(!this.ranges.isEmpty()) {
               RangeIterator<K, D> range = (RangeIterator)this.ranges.poll();
               if(!isOverlapping(head, range)) {
                  exhausted = true;
                  intersectsAll = false;
                  break;
               }

               D point = range.skipTo((Comparable)candidate.get());
               if(point == null) {
                  exhausted = true;
                  intersectsAll = false;
                  break;
               }

               processed.add(range);
               if(!((Comparable)candidate.get()).equals(point.get())) {
                  intersectsAll = false;
                  break;
               }

               candidate.merge(point);
               Iterators.getNext(range, (Object)null);
            }

            this.ranges.add(head);
            this.ranges.addAll(processed);
            processed.clear();
            if(exhausted) {
               return (CombinedValue)this.endOfData();
            }
         } while(!intersectsAll);

         return candidate;
      }

      protected void performSkipTo(K nextToken) {
         ArrayList skipped = new ArrayList();

         while(!this.ranges.isEmpty()) {
            RangeIterator<K, D> range = (RangeIterator)this.ranges.poll();
            range.skipTo(nextToken);
            skipped.add(range);
         }

         Iterator var5 = skipped.iterator();

         while(var5.hasNext()) {
            RangeIterator<K, D> range = (RangeIterator)var5.next();
            this.ranges.add(range);
         }

      }
   }

   private abstract static class AbstractIntersectionIterator<K extends Comparable<K>, D extends CombinedValue<K>> extends RangeIterator<K, D> {
      protected final PriorityQueue<RangeIterator<K, D>> ranges;

      private AbstractIntersectionIterator(RangeIterator.Builder.Statistics<K, D> statistics, PriorityQueue<RangeIterator<K, D>> ranges) {
         super(statistics);
         this.ranges = ranges;
      }

      public void close() throws IOException {
         Iterator var1 = this.ranges.iterator();

         while(var1.hasNext()) {
            RangeIterator<K, D> range = (RangeIterator)var1.next();
            FileUtils.closeQuietly((Closeable)range);
         }

      }
   }

   public static class Builder<K extends Comparable<K>, D extends CombinedValue<K>> extends RangeIterator.Builder<K, D> {
      private final RangeIntersectionIterator.Strategy strategy;

      public Builder(RangeIntersectionIterator.Strategy strategy) {
         super(RangeIterator.Builder.IteratorType.INTERSECTION);
         this.strategy = strategy;
      }

      protected RangeIterator<K, D> buildIterator() {
         if(this.statistics.isDisjoint()) {
            return new RangeIterator.Builder.EmptyRangeIterator();
         } else if(this.rangeCount() == 1) {
            return (RangeIterator)this.ranges.poll();
         } else {
            switch(null.$SwitchMap$org$apache$cassandra$index$sasi$utils$RangeIntersectionIterator$Strategy[this.strategy.ordinal()]) {
            case 1:
               return new RangeIntersectionIterator.LookupIntersectionIterator(this.statistics, this.ranges, null);
            case 2:
               return new RangeIntersectionIterator.BounceIntersectionIterator(this.statistics, this.ranges, null);
            case 3:
               return (RangeIterator)(this.statistics.sizeRatio() <= 0.01D?new RangeIntersectionIterator.LookupIntersectionIterator(this.statistics, this.ranges, null):new RangeIntersectionIterator.BounceIntersectionIterator(this.statistics, this.ranges, null));
            default:
               throw new IllegalStateException("Unknown strategy: " + this.strategy);
            }
         }
      }
   }

   protected static enum Strategy {
      BOUNCE,
      LOOKUP,
      ADAPTIVE;

      private Strategy() {
      }
   }
}
