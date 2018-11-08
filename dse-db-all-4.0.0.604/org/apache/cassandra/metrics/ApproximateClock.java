package org.apache.cassandra.metrics;

import com.codahale.metrics.Clock;
import org.apache.cassandra.db.monitoring.ApproximateTime;

class ApproximateClock extends Clock {
   private static final Clock DEFAULT = new ApproximateClock();

   ApproximateClock() {
   }

   public static Clock defaultClock() {
      return DEFAULT;
   }

   public long getTime() {
      return ApproximateTime.currentTimeMillis();
   }

   public long getTick() {
      return ApproximateTime.nanoTime();
   }
}
