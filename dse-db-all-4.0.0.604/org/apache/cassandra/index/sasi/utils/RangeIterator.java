package org.apache.cassandra.index.sasi.utils;

import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.function.Consumer;

public abstract class RangeIterator<K extends Comparable<K>, T extends CombinedValue<K>> extends AbstractIterator<T> implements Closeable {
   private final K min;
   private final K max;
   private final long count;
   private K current;

   protected RangeIterator(RangeIterator.Builder.Statistics<K, T> statistics) {
      this(statistics.min, statistics.max, statistics.tokenCount);
   }

   public RangeIterator(RangeIterator<K, T> range) {
      this(range == null?null:range.min, range == null?null:range.max, range == null?-1L:range.count);
   }

   public RangeIterator(K min, K max, long count) {
      assert min != null && max != null && count != 0L || min == null && max == null && (count == 0L || count == -1L);

      this.min = min;
      this.current = min;
      this.max = max;
      this.count = count;
   }

   public final K getMinimum() {
      return this.min;
   }

   public final K getCurrent() {
      return this.current;
   }

   public final K getMaximum() {
      return this.max;
   }

   public final long getCount() {
      return this.count;
   }

   public final T skipTo(K nextToken) {
      if(this.min != null && this.max != null) {
         if(this.current.compareTo(nextToken) >= 0) {
            return this.next == null?this.recomputeNext():(CombinedValue)this.next;
         } else if(this.max.compareTo(nextToken) < 0) {
            return (CombinedValue)this.endOfData();
         } else {
            this.performSkipTo(nextToken);
            return this.recomputeNext();
         }
      } else {
         return (CombinedValue)this.endOfData();
      }
   }

   protected abstract void performSkipTo(K var1);

   protected T recomputeNext() {
      return this.tryToComputeNext()?(CombinedValue)this.peek():(CombinedValue)this.endOfData();
   }

   protected boolean tryToComputeNext() {
      boolean hasNext = super.tryToComputeNext();
      this.current = hasNext?(Comparable)((CombinedValue)this.next).get():this.getMaximum();
      return hasNext;
   }

   @VisibleForTesting
   protected static <K extends Comparable<K>, D extends CombinedValue<K>> boolean isOverlapping(RangeIterator<K, D> a, RangeIterator<K, D> b) {
      return isOverlapping(a.getCurrent(), a.getMaximum(), b);
   }

   @VisibleForTesting
   protected static <K extends Comparable<K>, D extends CombinedValue<K>> boolean isOverlapping(K min, K max, RangeIterator<K, D> b) {
      return min != null && max != null && b.getCount() != 0L && min.compareTo(b.getMaximum()) <= 0 && b.getCurrent().compareTo(max) <= 0;
   }

   private static <T extends Comparable> T nullSafeMin(T a, T b) {
      return a == null?b:(b == null?a:(a.compareTo(b) > 0?b:a));
   }

   private static <T extends Comparable> T nullSafeMax(T a, T b) {
      return a == null?b:(b == null?a:(a.compareTo(b) > 0?a:b));
   }

   public abstract static class Builder<K extends Comparable<K>, D extends CombinedValue<K>> {
      @VisibleForTesting
      protected final RangeIterator.Builder.Statistics<K, D> statistics;
      @VisibleForTesting
      protected final PriorityQueue<RangeIterator<K, D>> ranges;

      public Builder(RangeIterator.Builder.IteratorType type) {
         this.statistics = new RangeIterator.Builder.Statistics(type);
         this.ranges = new PriorityQueue(16, (a, b) -> {
            return a.getCurrent().compareTo(b.getCurrent());
         });
      }

      public K getMinimum() {
         return this.statistics.min;
      }

      public K getMaximum() {
         return this.statistics.max;
      }

      public long getTokenCount() {
         return this.statistics.tokenCount;
      }

      public int rangeCount() {
         return this.ranges.size();
      }

      public RangeIterator.Builder<K, D> add(RangeIterator<K, D> range) {
         if(range == null) {
            return this;
         } else {
            if(range.getCount() > 0L) {
               this.ranges.add(range);
            }

            this.statistics.update(range);
            return this;
         }
      }

      public RangeIterator.Builder<K, D> add(List<RangeIterator<K, D>> ranges) {
         if(ranges != null && !ranges.isEmpty()) {
            ranges.forEach(this::add);
            return this;
         } else {
            return this;
         }
      }

      public final RangeIterator<K, D> build() {
         return (RangeIterator)(this.rangeCount() == 0?new RangeIterator.Builder.EmptyRangeIterator():this.buildIterator());
      }

      protected abstract RangeIterator<K, D> buildIterator();

      public static class Statistics<K extends Comparable<K>, D extends CombinedValue<K>> {
         protected final RangeIterator.Builder.IteratorType iteratorType;
         protected K min;
         protected K max;
         protected long tokenCount;
         protected RangeIterator<K, D> minRange;
         protected RangeIterator<K, D> maxRange;
         private boolean isOverlapping = true;

         public Statistics(RangeIterator.Builder.IteratorType iteratorType) {
            this.iteratorType = iteratorType;
         }

         public void update(RangeIterator<K, D> range) {
            switch(null.$SwitchMap$org$apache$cassandra$index$sasi$utils$RangeIterator$Builder$IteratorType[this.iteratorType.ordinal()]) {
            case 1:
               this.min = RangeIterator.nullSafeMin(this.min, range.getMinimum());
               this.max = RangeIterator.nullSafeMax(this.max, range.getMaximum());
               break;
            case 2:
               this.min = RangeIterator.nullSafeMax(this.min, range.getMinimum());
               this.max = RangeIterator.nullSafeMin(this.max, range.getMaximum());
               break;
            default:
               throw new IllegalStateException("Unknown iterator type: " + this.iteratorType);
            }

            this.isOverlapping &= RangeIterator.isOverlapping(this.min, this.max, range);
            this.minRange = this.minRange == null?range:this.min(this.minRange, range);
            this.maxRange = this.maxRange == null?range:this.max(this.maxRange, range);
            this.tokenCount += range.getCount();
         }

         private RangeIterator<K, D> min(RangeIterator<K, D> a, RangeIterator<K, D> b) {
            return a.getCount() > b.getCount()?b:a;
         }

         private RangeIterator<K, D> max(RangeIterator<K, D> a, RangeIterator<K, D> b) {
            return a.getCount() > b.getCount()?a:b;
         }

         public boolean isDisjoint() {
            return !this.isOverlapping;
         }

         public double sizeRatio() {
            return (double)this.minRange.getCount() * 1.0D / (double)this.maxRange.getCount();
         }
      }

      public static class EmptyRangeIterator<K extends Comparable<K>, D extends CombinedValue<K>> extends RangeIterator<K, D> {
         EmptyRangeIterator() {
            super((Comparable)null, (Comparable)null, 0L);
         }

         public D computeNext() {
            return (CombinedValue)this.endOfData();
         }

         protected void performSkipTo(K nextToken) {
         }

         public void close() {
         }
      }

      public static enum IteratorType {
         UNION,
         INTERSECTION;

         private IteratorType() {
         }
      }
   }
}
