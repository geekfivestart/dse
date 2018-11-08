package org.apache.cassandra.metrics;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

public class EWMA {
   private static final int INTERVAL = 5;
   private static final double SECONDS_PER_MINUTE = 60.0D;
   private static final int ONE_MINUTE = 1;
   private static final int FIVE_MINUTES = 5;
   private static final int FIFTEEN_MINUTES = 15;
   private static final double M1_ALPHA = 1.0D - Math.exp(-0.08333333333333333D);
   private static final double M5_ALPHA = 1.0D - Math.exp(-0.016666666666666666D);
   private static final double M15_ALPHA = 1.0D - Math.exp(-0.005555555555555555D);
   private final LongAdder uncounted;
   private volatile boolean initialized = false;
   private volatile double rate = 0.0D;
   private final double alpha;
   private final double interval;

   public static EWMA oneMinuteEWMA(boolean keepCount) {
      return new EWMA(M1_ALPHA, 5L, TimeUnit.SECONDS, keepCount);
   }

   public static EWMA fiveMinuteEWMA(boolean keepCount) {
      return new EWMA(M5_ALPHA, 5L, TimeUnit.SECONDS, keepCount);
   }

   public static EWMA fifteenMinuteEWMA(boolean keepCount) {
      return new EWMA(M15_ALPHA, 5L, TimeUnit.SECONDS, keepCount);
   }

   public EWMA(double alpha, long interval, TimeUnit intervalUnit, boolean keepCount) {
      this.interval = (double)intervalUnit.toNanos(interval);
      this.alpha = alpha;
      this.uncounted = keepCount?new LongAdder():null;
   }

   public void update(long n) {
      if(this.uncounted == null) {
         throw new UnsupportedOperationException();
      } else {
         this.uncounted.add(n);
      }
   }

   public void tick() {
      if(this.uncounted == null) {
         throw new UnsupportedOperationException();
      } else {
         this.tick(this.uncounted.sumThenReset());
      }
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

   public double getRate(TimeUnit rateUnit) {
      return this.rate * (double)rateUnit.toNanos(1L);
   }
}
