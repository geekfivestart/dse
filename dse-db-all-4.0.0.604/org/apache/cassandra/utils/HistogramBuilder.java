package org.apache.cassandra.utils;

import java.util.Arrays;

public class HistogramBuilder {
   public static final long[] EMPTY_LONG_ARRAY = new long[0];
   public static final long[] ZERO = new long[]{0L};
   private long[] values = new long[10];
   int count = 0;

   public HistogramBuilder() {
   }

   public HistogramBuilder(long[] values) {
      long[] var2 = values;
      int var3 = values.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         long value = var2[var4];
         this.add(value);
      }

   }

   public void add(long value) {
      if(this.count == this.values.length) {
         this.values = Arrays.copyOf(this.values, this.values.length << 1);
      }

      this.values[this.count++] = value;
   }

   public EstimatedHistogram buildWithStdevRangesAroundMean() {
      return this.buildWithStdevRangesAroundMean(3);
   }

   public EstimatedHistogram buildWithStdevRangesAroundMean(int maxdevs) {
      if(maxdevs < 0) {
         throw new IllegalArgumentException("maxdevs must be greater than or equal to zero");
      } else {
         int count = this.count;
         long[] values = this.values;
         if(count == 0) {
            return new EstimatedHistogram(EMPTY_LONG_ARRAY, ZERO);
         } else {
            long min = 9223372036854775807L;
            long max = -9223372036854775808L;
            double sum = 0.0D;
            double sumsq = 0.0D;

            for(int i = 0; i < count; ++i) {
               long value = values[i];
               sum += (double)value;
               sumsq += (double)(value * value);
               if(value < min) {
                  min = value;
               }

               if(value > max) {
                  max = value;
               }
            }

            long mean = Math.round(sum / (double)count);
            double stdev = Math.sqrt(sumsq / (double)count - (double)mean * (double)mean);
            long[] lowhalf = buildRange(mean, min, true, stdev, maxdevs);
            long[] highhalf = buildRange(mean, max, false, stdev, maxdevs);
            long[] ranges = new long[lowhalf.length + highhalf.length + 1];
            System.arraycopy(lowhalf, 0, ranges, 0, lowhalf.length);
            ranges[lowhalf.length] = mean;
            System.arraycopy(highhalf, 0, ranges, lowhalf.length + 1, highhalf.length);
            EstimatedHistogram hist = new EstimatedHistogram(ranges, new long[ranges.length + 1]);

            for(int i = 0; i < count; ++i) {
               hist.add(values[i]);
            }

            return hist;
         }
      }
   }

   private static long[] buildRange(long mean, long minormax, boolean ismin, double stdev, int maxdevs) {
      if(minormax == mean) {
         return ismin?new long[]{mean - 1L}:EMPTY_LONG_ARRAY;
      } else if(stdev < 1.0D) {
         return ismin?new long[]{minormax - 1L, mean - 1L}:new long[]{minormax};
      } else {
         long larger;
         long smaller;
         if(ismin) {
            larger = mean;
            smaller = minormax;
         } else {
            larger = minormax;
            smaller = mean;
         }

         double stdevsTo = (double)(larger - smaller) / stdev;
         if(stdevsTo > 0.0D && stdevsTo < 1.0D) {
            stdevsTo = 1.0D;
         } else {
            stdevsTo = (double)Math.round(stdevsTo);
         }

         int len = Math.min(maxdevs + 1, (int)stdevsTo);
         long[] range = new long[len];
         long next = ismin?minormax - 1L:minormax;

         for(int i = 0; i < range.length; ++i) {
            long delta = (long)(range.length - (i + 1)) * (long)stdev;
            if(ismin) {
               range[i] = next;
               next = mean - delta;
            } else {
               range[len - 1 - i] = next;
               next = mean + delta;
            }
         }

         return range;
      }
   }
}
