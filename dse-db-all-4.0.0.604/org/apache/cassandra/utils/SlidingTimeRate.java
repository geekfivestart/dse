package org.apache.cassandra.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BinaryOperator;

public class SlidingTimeRate {
   private final ConcurrentSkipListMap<Long, AtomicInteger> counters = new ConcurrentSkipListMap();
   private final AtomicLong lastCounterTimestamp = new AtomicLong(0L);
   private final long sizeInMillis;
   private final long precisionInMillis;
   private final TimeSource timeSource;

   public SlidingTimeRate(TimeSource timeSource, long size, long precision, TimeUnit unit) {
      Preconditions.checkArgument(size > precision, "Size should be greater than precision.");
      Preconditions.checkArgument(TimeUnit.MILLISECONDS.convert(precision, unit) >= 1L, "Precision must be greater than or equal to 1 millisecond.");
      this.sizeInMillis = TimeUnit.MILLISECONDS.convert(size, unit);
      this.precisionInMillis = TimeUnit.MILLISECONDS.convert(precision, unit);
      this.timeSource = timeSource;
   }

   public void update(int delta) {
      while(true) {
         long now = this.timeSource.currentTimeMillis();
         long lastTimestamp = this.lastCounterTimestamp.get();
         boolean isWithinPrecisionRange = now - lastTimestamp < this.precisionInMillis;
         AtomicInteger lastCounter = (AtomicInteger)this.counters.get(Long.valueOf(lastTimestamp));
         if(lastCounter != null && isWithinPrecisionRange) {
            lastCounter.addAndGet(delta);
         } else {
            if(!this.lastCounterTimestamp.compareAndSet(lastTimestamp, now)) {
               continue;
            }

            AtomicInteger existing = (AtomicInteger)this.counters.putIfAbsent(Long.valueOf(now), new AtomicInteger(delta));
            if(existing != null) {
               existing.addAndGet(delta);
            }
         }

         return;
      }
   }

   public double get(long toAgo, TimeUnit unit) {
      long toAgoInMillis = TimeUnit.MILLISECONDS.convert(toAgo, unit);
      Preconditions.checkArgument(toAgoInMillis < this.sizeInMillis, "Cannot get rate in the past!");
      long now = this.timeSource.currentTimeMillis();
      long sum = 0L;
      ConcurrentNavigableMap<Long, AtomicInteger> tailCounters = this.counters.tailMap(Long.valueOf(now - this.sizeInMillis), true).headMap(Long.valueOf(now - toAgoInMillis), true);

      AtomicInteger i;
      for(Iterator var11 = tailCounters.values().iterator(); var11.hasNext(); sum += (long)i.get()) {
         i = (AtomicInteger)var11.next();
      }

      double multiplier = (double)TimeUnit.MILLISECONDS.convert(1L, unit);
      double rateInMillis = sum == 0L?(double)sum:(double)sum / Math.max(multiplier, (double)(now - toAgoInMillis - ((Long)tailCounters.firstKey()).longValue()));
      return rateInMillis * multiplier;
   }

   public double get(TimeUnit unit) {
      return this.get(0L, unit);
   }

   public void prune() {
      long now = this.timeSource.currentTimeMillis();
      this.counters.headMap(Long.valueOf(now - this.sizeInMillis), false).clear();
   }

   @VisibleForTesting
   public int size() {
      return ((AtomicInteger)this.counters.values().stream().reduce(new AtomicInteger(), (v1, v2) -> {
         v1.addAndGet(v2.get());
         return v1;
      })).get();
   }
}
