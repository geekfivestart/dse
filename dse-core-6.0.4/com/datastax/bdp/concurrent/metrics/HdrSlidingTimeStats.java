package com.datastax.bdp.concurrent.metrics;

import com.datastax.bdp.system.TimeSource;
import com.google.common.base.Preconditions;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.validation.constraints.NotNull;
import org.HdrHistogram.AtomicHistogram;
import org.HdrHistogram.Histogram;

public class HdrSlidingTimeStats implements SlidingTimeStats {
   private final long granularity;
   private final long length;
   private final long maxValue;
   private final int significantValueDigits;
   private final ConcurrentNavigableMap<Long, AtomicHistogram> histograms;
   private final AtomicLong lastInterval;
   private final TimeSource timeSource;

   public HdrSlidingTimeStats(@NotNull TimeSource timeSource, long granularity, long length, long maxValue, @NotNull TimeUnit unit, int significantValueDigits) {
      Preconditions.checkNotNull(timeSource);
      Preconditions.checkNotNull(unit);
      this.timeSource = timeSource;
      this.granularity = TimeUnit.MILLISECONDS.convert(granularity, unit);
      Preconditions.checkArgument(this.granularity > 0L, "Granularity " + this.granularity + " must be >= 1 millisecond!");
      this.length = TimeUnit.MILLISECONDS.convert(length, unit);
      Preconditions.checkArgument(this.length > 0L, "Length " + this.length + " must be >= 1 millisecond!");
      Preconditions.checkArgument(length != granularity && length % granularity == 0L, "Length " + length + " must be a multiple of granularity " + granularity);
      Preconditions.checkArgument(significantValueDigits >= 0 && significantValueDigits <= 5, "Significant value digits(" + significantValueDigits + ") must be >= 0 and <= 5");
      this.maxValue = TimeUnit.MICROSECONDS.convert(maxValue, unit);
      this.histograms = new ConcurrentSkipListMap();
      this.lastInterval = new AtomicLong();
      this.significantValueDigits = significantValueDigits;
   }

   public void update(long value, TimeUnit unit) {
      while(true) {
         long currentLastInterval = this.lastInterval.get();
         long now = this.timeSource.currentTimeMillis();
         AtomicHistogram histogram;
         if(now - currentLastInterval > this.granularity) {
            if(!this.lastInterval.compareAndSet(currentLastInterval, now)) {
               continue;
            }

            histogram = new AtomicHistogram(this.maxValue, this.significantValueDigits);
            histogram.recordValue(TimeUnit.MICROSECONDS.convert(value, unit));
            this.histograms.put(Long.valueOf(now), histogram);
         } else {
            histogram = (AtomicHistogram)this.histograms.get(Long.valueOf(currentLastInterval));
            if(histogram == null) {
               continue;
            }

            histogram.recordValue(TimeUnit.MICROSECONDS.convert(value, unit));
         }

         this.prune();
         return;
      }
   }

   public double computeAverage() {
      Histogram snapshot = new Histogram(this.maxValue, this.significantValueDigits);
      long startOfCurrentWindow = this.timeSource.currentTimeMillis() - this.length;
      Iterator var4 = this.histograms.tailMap(Long.valueOf(startOfCurrentWindow), true).values().iterator();

      while(var4.hasNext()) {
         AtomicHistogram histogram = (AtomicHistogram)var4.next();
         snapshot.add(histogram);
      }

      double avg = snapshot.getMean();
      return Double.isNaN(avg)?0.0D:avg;
   }

   public int sizeInBytes() {
      int size = 0;

      AtomicHistogram histogram;
      for(Iterator var2 = this.histograms.values().iterator(); var2.hasNext(); size += histogram.getEstimatedFootprintInBytes()) {
         histogram = (AtomicHistogram)var2.next();
      }

      return size;
   }

   private void prune() {
      long startOfCurrentWindow = this.timeSource.currentTimeMillis() - this.length;
      this.histograms.headMap(Long.valueOf(startOfCurrentWindow)).clear();
   }
}
