package org.apache.cassandra.utils;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.stream.Collectors;

public class IntegerInterval {
   volatile long interval;
   private static AtomicLongFieldUpdater<IntegerInterval> intervalUpdater = AtomicLongFieldUpdater.newUpdater(IntegerInterval.class, "interval");

   private IntegerInterval(long interval) {
      this.interval = interval;
   }

   public IntegerInterval(int lower, int upper) {
      this(make(lower, upper));
   }

   public IntegerInterval(IntegerInterval src) {
      this(src.interval);
   }

   public int lower() {
      return lower(this.interval);
   }

   public int upper() {
      return upper(this.interval);
   }

   public void expandToCover(int value) {
      long prev;
      int lower;
      int upper;
      do {
         prev = this.interval;
         upper = upper(prev);
         lower = lower(prev);
         if(value > upper) {
            upper = value;
         } else if(value < lower) {
            lower = value;
         }
      } while(!intervalUpdater.compareAndSet(this, prev, make(lower, upper)));

   }

   public int hashCode() {
      return Long.hashCode(this.interval);
   }

   public boolean equals(Object obj) {
      if(this.getClass() != obj.getClass()) {
         return false;
      } else {
         IntegerInterval other = (IntegerInterval)obj;
         return this.interval == other.interval;
      }
   }

   public String toString() {
      long interval = this.interval;
      return "[" + lower(interval) + "," + upper(interval) + "]";
   }

   private static long make(int lower, int upper) {
      assert lower <= upper;

      return ((long)lower & 4294967295L) << 32 | (long)upper & 4294967295L;
   }

   private static int lower(long interval) {
      return (int)(interval >>> 32);
   }

   private static int upper(long interval) {
      return (int)interval;
   }

   public static class Set {
      static long[] EMPTY = new long[0];
      private volatile long[] ranges;

      public Set() {
         this.ranges = EMPTY;
      }

      public synchronized void add(int start, int end) {
         assert start <= end;

         long[] ranges = this.ranges;
         int rpos = Arrays.binarySearch(ranges, ((long)end & 4294967295L) << 32 | 4294967295L);
         if(rpos < 0) {
            rpos = -1 - rpos - 1;
         }

         int lpos;
         if(rpos >= 0) {
            lpos = IntegerInterval.upper(ranges[rpos]);
            if(lpos > end) {
               end = lpos;
            }
         }

         lpos = Arrays.binarySearch(ranges, ((long)start & 4294967295L) << 32 | 0L);
         if(lpos < 0) {
            lpos = -1 - lpos;
         }

         --lpos;
         if(lpos >= 0 && IntegerInterval.upper(ranges[lpos]) >= start) {
            start = IntegerInterval.lower(ranges[lpos]);
            --lpos;
         }

         long[] newRanges = new long[ranges.length - (rpos - lpos) + 1];
         int dest = 0;

         int i;
         for(i = 0; i <= lpos; ++i) {
            newRanges[dest++] = ranges[i];
         }

         newRanges[dest++] = IntegerInterval.make(start, end);

         for(i = rpos + 1; i < ranges.length; ++i) {
            newRanges[dest++] = ranges[i];
         }

         this.ranges = newRanges;
      }

      public boolean covers(IntegerInterval iv) {
         long l = iv.interval;
         return this.covers(IntegerInterval.lower(l), IntegerInterval.upper(l));
      }

      public boolean covers(int start, int end) {
         long[] ranges = this.ranges;
         int rpos = Arrays.binarySearch(ranges, ((long)start & 4294967295L) << 32 | 4294967295L);
         if(rpos < 0) {
            rpos = -1 - rpos - 1;
         }

         return rpos == -1?false:IntegerInterval.upper(ranges[rpos]) >= end;
      }

      public int lowerBound() {
         return IntegerInterval.lower(this.ranges[0]);
      }

      public int upperBound() {
         long[] ranges = this.ranges;
         return IntegerInterval.upper(ranges[ranges.length - 1]);
      }

      public Collection<IntegerInterval> intervals() {
         return Lists.transform(Longs.asList(this.ranges), (iv) -> {
            return new IntegerInterval(iv.longValue());
         });
      }

      public int hashCode() {
         return Arrays.hashCode(this.ranges);
      }

      public boolean equals(Object obj) {
         if(this.getClass() != obj.getClass()) {
            return false;
         } else {
            IntegerInterval.Set other = (IntegerInterval.Set)obj;
            return Arrays.equals(this.ranges, other.ranges);
         }
      }

      public String toString() {
         return "[" + (String)this.intervals().stream().map(IntegerInterval::toString).collect(Collectors.joining(", ")) + "]";
      }
   }
}
