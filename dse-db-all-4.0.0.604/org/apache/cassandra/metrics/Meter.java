package org.apache.cassandra.metrics;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Metered;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.cassandra.concurrent.TPC;

public class Meter extends com.codahale.metrics.Meter implements Metered, Composable<Meter> {
   private static final long TICK_INTERVAL;
   private final EWMA m1Rate;
   private final EWMA m5Rate;
   private final EWMA m15Rate;
   private final Clock clock;
   private final AtomicLong lastCounted;
   private final Counter count;
   private final long startTime;
   private final AtomicLong lastTick;
   private final AtomicBoolean scheduled;

   public Meter() {
      this(false);
   }

   public Meter(boolean isComposite) {
      this(Clock.defaultClock(), Counter.make(isComposite));
   }

   public Meter(Clock clock, Counter count) {
      this.m1Rate = EWMA.oneMinuteEWMA(false);
      this.m5Rate = EWMA.fiveMinuteEWMA(false);
      this.m15Rate = EWMA.fifteenMinuteEWMA(false);
      this.lastCounted = new AtomicLong();
      this.scheduled = new AtomicBoolean(false);
      this.clock = clock;
      this.count = count;
      this.startTime = this.clock.getTick();
      this.lastTick = new AtomicLong(this.startTime);
      this.scheduleIfComposite();
   }

   private void scheduleIfComposite() {
      if(this.count.getType() == Composable.Type.COMPOSITE) {
         TPC.bestTPCTimer().onTimeout(this::scheduledTick, TICK_INTERVAL, TimeUnit.NANOSECONDS);
      }

   }

   private void maybeScheduleTick() {
      if(!this.scheduled.get()) {
         if(this.scheduled.compareAndSet(false, true)) {
            TPC.bestTPCTimer().onTimeout(this::scheduledTick, TICK_INTERVAL, TimeUnit.NANOSECONDS);
         }

      }
   }

   public void mark() {
      this.mark(1L);
   }

   public void mark(long n) {
      this.maybeScheduleTick();
      this.count.inc(n);
   }

   private void scheduledTick() {
      try {
         this.scheduled.set(false);
         this.tickIfNecessary();
      } finally {
         this.scheduleIfComposite();
      }

   }

   private void tickIfNecessary() {
      long oldTick = this.lastTick.get();
      long newTick = this.clock.getTick();
      long age = newTick - oldTick;
      if(age > TICK_INTERVAL) {
         long newIntervalStartTick = newTick - age % TICK_INTERVAL;
         if(this.lastTick.compareAndSet(oldTick, newIntervalStartTick)) {
            long requiredTicks = age / TICK_INTERVAL;

            for(long i = 0L; i < requiredTicks; ++i) {
               long count = this.getUncounted();
               this.m1Rate.tick(count);
               this.m5Rate.tick(count);
               this.m15Rate.tick(count);
            }
         }
      }

   }

   private long getUncounted() {
      long current = this.getCount();
      long previous = this.lastCounted.getAndSet(current);
      return current - previous;
   }

   public long getCount() {
      return this.count.getCount();
   }

   public double getFifteenMinuteRate() {
      this.tickIfNecessary();
      return this.m15Rate.getRate(TimeUnit.SECONDS);
   }

   public double getFiveMinuteRate() {
      this.tickIfNecessary();
      return this.m5Rate.getRate(TimeUnit.SECONDS);
   }

   public double getMeanRate() {
      return this.getMeanRate(this.getCount());
   }

   public double getMeanRate(long count) {
      if(count == 0L) {
         return 0.0D;
      } else {
         double elapsed = (double)(this.clock.getTick() - this.startTime);
         return (double)count / elapsed * (double)TimeUnit.SECONDS.toNanos(1L);
      }
   }

   public double getOneMinuteRate() {
      this.tickIfNecessary();
      return this.m1Rate.getRate(TimeUnit.SECONDS);
   }

   public Composable.Type getType() {
      return this.count.getType();
   }

   public void compose(Meter metric) {
      this.count.compose(metric.count);
   }

   public String toString() {
      return String.format("count: %d, 1-min: %.2f, 5-min: %.2f, 15-min: %.2f", new Object[]{Long.valueOf(this.getCount()), Double.valueOf(this.getOneMinuteRate()), Double.valueOf(this.getFiveMinuteRate()), Double.valueOf(this.getFiveMinuteRate())});
   }

   static {
      TICK_INTERVAL = TimeUnit.SECONDS.toNanos(5L);
   }
}
