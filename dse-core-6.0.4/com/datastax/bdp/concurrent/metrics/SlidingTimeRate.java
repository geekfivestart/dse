package com.datastax.bdp.concurrent.metrics;

import com.datastax.bdp.system.SystemTimeSource;
import com.datastax.bdp.system.TimeSource;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SlidingTimeRate {
   private final ConcurrentSkipListMap<Long, AtomicInteger> counters;
   private final AtomicLong lastCounterTimestamp;
   private final long sizeInMillis;
   private final long precisionInMillis;
   private final TimeSource timeSource;

   public SlidingTimeRate(int size, int precision, TimeUnit unit) {
      this(new SystemTimeSource(), size, precision, unit);
   }

   public SlidingTimeRate(TimeSource timeSource, int size, int precision, TimeUnit unit) {
      this.counters = new ConcurrentSkipListMap();
      this.lastCounterTimestamp = new AtomicLong(0L);
      Preconditions.checkArgument(size > precision, "Size should be greater than precision.");
      Preconditions.checkArgument(TimeUnit.MILLISECONDS.convert((long)precision, unit) >= 1L, "Precision must be greater than or equal to 1 millisecond.");
      this.sizeInMillis = TimeUnit.MILLISECONDS.convert((long)size, unit);
      this.precisionInMillis = TimeUnit.MILLISECONDS.convert((long)precision, unit);
      this.timeSource = timeSource;
   }

   public void update(int delta) {
      while(true) {
         long now = this.timeSource.currentTimeMillis();
         long lastTimestamp = this.lastCounterTimestamp.get();
         boolean isWithinPrecisionRange = now - lastTimestamp <= this.precisionInMillis;
         AtomicInteger lastCounter = (AtomicInteger)this.counters.get(Long.valueOf(lastTimestamp));
         if(lastCounter != null && isWithinPrecisionRange) {
            lastCounter.addAndGet(delta);
         } else {
            if(isWithinPrecisionRange || !this.lastCounterTimestamp.compareAndSet(lastTimestamp, now)) {
               continue;
            }

            this.counters.put(Long.valueOf(now), new AtomicInteger(delta));
         }

         return;
      }
   }

   public double get(TimeUnit unit) {
      long now = this.timeSource.currentTimeMillis();
      long sum = 0L;
      ConcurrentNavigableMap<Long, AtomicInteger> tailCounters = this.counters.tailMap(Long.valueOf(now - this.sizeInMillis), true);

      AtomicInteger i;
      for(Iterator var7 = tailCounters.values().iterator(); var7.hasNext(); sum += (long)i.get()) {
         i = (AtomicInteger)var7.next();
      }

      return sum == 0L?(double)sum:(double)sum / (double)Math.max(1L, unit.convert(now - ((Long)tailCounters.firstKey()).longValue(), TimeUnit.MILLISECONDS));
   }

   public void prune() {
      long now = this.timeSource.currentTimeMillis();
      this.counters.headMap(Long.valueOf(now - this.sizeInMillis), false).clear();
   }

   @VisibleForTesting
   public int size() {
      return this.counters.size();
   }
}
