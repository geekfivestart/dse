package org.apache.cassandra.index.sasi.utils;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.PriorityQueue;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.cassandra.io.util.FileUtils;

public class RangeUnionIterator<K extends Comparable<K>, D extends CombinedValue<K>> extends RangeIterator<K, D> {
   private final PriorityQueue<RangeIterator<K, D>> ranges;

   private RangeUnionIterator(RangeIterator.Builder.Statistics<K, D> statistics, PriorityQueue<RangeIterator<K, D>> ranges) {
      super(statistics);
      this.ranges = ranges;
   }

   public D computeNext() {
      RangeIterator head = null;

      while(!this.ranges.isEmpty()) {
         head = (RangeIterator)this.ranges.poll();
         if(head.hasNext()) {
            break;
         }

         FileUtils.closeQuietly((Closeable)head);
      }

      if(head != null && head.hasNext()) {
         D candidate = (CombinedValue)head.next();
         List<RangeIterator<K, D>> processedRanges = new ArrayList();
         if(head.hasNext()) {
            processedRanges.add(head);
         } else {
            FileUtils.closeQuietly((Closeable)head);
         }

         while(!this.ranges.isEmpty()) {
            RangeIterator<K, D> range = (RangeIterator)this.ranges.peek();
            int cmp = ((Comparable)candidate.get()).compareTo(range.getCurrent());

            assert cmp <= 0;

            if(cmp < 0) {
               break;
            }

            if(cmp == 0) {
               candidate.merge((CombinedValue)range.next());
               range = (RangeIterator)this.ranges.poll();
               if(range.hasNext()) {
                  processedRanges.add(range);
               } else {
                  FileUtils.closeQuietly((Closeable)range);
               }
            }
         }

         this.ranges.addAll(processedRanges);
         return candidate;
      } else {
         return (CombinedValue)this.endOfData();
      }
   }

   protected void performSkipTo(K nextToken) {
      ArrayList changedRanges = new ArrayList();

      while(!this.ranges.isEmpty() && ((RangeIterator)this.ranges.peek()).getCurrent().compareTo(nextToken) < 0) {
         RangeIterator<K, D> head = (RangeIterator)this.ranges.poll();
         if(head.getMaximum().compareTo(nextToken) >= 0) {
            head.skipTo(nextToken);
            changedRanges.add(head);
         } else {
            FileUtils.closeQuietly((Closeable)head);
         }
      }

      this.ranges.addAll((Collection)changedRanges.stream().collect(Collectors.toList()));
   }

   public void close() throws IOException {
      this.ranges.forEach(FileUtils::closeQuietly);
   }

   public static <K extends Comparable<K>, D extends CombinedValue<K>> RangeUnionIterator.Builder<K, D> builder() {
      return new RangeUnionIterator.Builder();
   }

   public static <K extends Comparable<K>, D extends CombinedValue<K>> RangeIterator<K, D> build(List<RangeIterator<K, D>> tokens) {
      return (new RangeUnionIterator.Builder()).add(tokens).build();
   }

   public static class Builder<K extends Comparable<K>, D extends CombinedValue<K>> extends RangeIterator.Builder<K, D> {
      public Builder() {
         super(RangeIterator.Builder.IteratorType.UNION);
      }

      protected RangeIterator<K, D> buildIterator() {
         switch(this.rangeCount()) {
         case 1:
            return (RangeIterator)this.ranges.poll();
         default:
            return new RangeUnionIterator(this.statistics, this.ranges);
         }
      }
   }
}
