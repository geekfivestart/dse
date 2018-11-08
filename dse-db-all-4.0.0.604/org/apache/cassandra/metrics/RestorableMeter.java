package org.apache.cassandra.metrics;

import com.codahale.metrics.Clock;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class RestorableMeter {
   private static final long TICK_INTERVAL;
   private static final double NANOS_PER_SECOND;
   private final RestorableMeter.RestorableEWMA m15Rate;
   private final RestorableMeter.RestorableEWMA m120Rate;
   private final Counter count = Counter.make(false);
   private final long startTime;
   private final AtomicLong lastTick;
   private final Clock clock = ApproximateClock.defaultClock();
   private final AtomicLong lastCounted = new AtomicLong();

   public RestorableMeter() {
      this.m15Rate = new RestorableMeter.RestorableEWMA(TimeUnit.MINUTES.toSeconds(15L));
      this.m120Rate = new RestorableMeter.RestorableEWMA(TimeUnit.MINUTES.toSeconds(120L));
      this.startTime = this.clock.getTick();
      this.lastTick = new AtomicLong(this.startTime);
   }

   public RestorableMeter(double lastM15Rate, double lastM120Rate) {
      this.m15Rate = new RestorableMeter.RestorableEWMA(lastM15Rate, TimeUnit.MINUTES.toSeconds(15L));
      this.m120Rate = new RestorableMeter.RestorableEWMA(lastM120Rate, TimeUnit.MINUTES.toSeconds(120L));
      this.startTime = this.clock.getTick();
      this.lastTick = new AtomicLong(this.startTime);
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
               this.m15Rate.tick(count);
               this.m120Rate.tick(count);
            }
         }
      }

   }

   private long getUncounted() {
      long current = this.count();
      long previous = this.lastCounted.getAndSet(current);
      return current - previous;
   }

   public void mark() {
      this.mark(1L);
   }

   public void mark(long n) {
      this.tickIfNecessary();
      this.count.inc(n);
   }

   public double fifteenMinuteRate() {
      this.tickIfNecessary();
      return this.m15Rate.rate();
   }

   public double twoHourRate() {
      this.tickIfNecessary();
      return this.m120Rate.rate();
   }

   public long count() {
      return this.count.getCount();
   }

   public double meanRate() {
      if(this.count() == 0L) {
         return 0.0D;
      } else {
         long elapsed = this.clock.getTick() - this.startTime;
         return (double)this.count() / (double)elapsed * NANOS_PER_SECOND;
      }
   }

   static {
      TICK_INTERVAL = TimeUnit.SECONDS.toNanos(5L);
      NANOS_PER_SECOND = (double)TimeUnit.SECONDS.toNanos(1L);
   }

   static class RestorableEWMA {
      private volatile boolean initialized;
      private volatile double rate;
      private final double alpha;
      private final double interval;

      public RestorableEWMA(long windowInSeconds) {
         this.initialized = false;
         this.rate = 0.0D;
         this.alpha = 1.0D - Math.exp((double)(-RestorableMeter.TICK_INTERVAL) / RestorableMeter.NANOS_PER_SECOND / (double)windowInSeconds);
         this.interval = (double)RestorableMeter.TICK_INTERVAL;
      }

      public RestorableEWMA(double lastRate, long intervalInSeconds) {
         this(intervalInSeconds);
         this.rate = lastRate / RestorableMeter.NANOS_PER_SECOND;
         this.initialized = true;
      }

      public void tick(long count) {
         double instantRate = (double)count / this.interval;
         if(this.initialized) {
            this.rate += this.alpha * (instantRate - this.rate);
         } else {
            this.rate = instantRate;
            this.initialized = true;
         }

      }

      public double rate() {
         return this.rate * RestorableMeter.NANOS_PER_SECOND;
      }
   }
}
