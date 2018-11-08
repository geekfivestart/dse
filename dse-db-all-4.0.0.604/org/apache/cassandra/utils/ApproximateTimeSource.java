package org.apache.cassandra.utils;

import com.google.common.util.concurrent.Uninterruptibles;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.db.monitoring.ApproximateTime;

public class ApproximateTimeSource implements TimeSource {
   public ApproximateTimeSource() {
   }

   public long currentTimeMillis() {
      return ApproximateTime.currentTimeMillis();
   }

   public long nanoTime() {
      return ApproximateTime.nanoTime();
   }

   public TimeSource sleepUninterruptibly(long sleepFor, TimeUnit unit) {
      Uninterruptibles.sleepUninterruptibly(sleepFor, unit);
      return this;
   }

   public TimeSource sleep(long sleepFor, TimeUnit unit) throws InterruptedException {
      TimeUnit.NANOSECONDS.sleep(TimeUnit.NANOSECONDS.convert(sleepFor, unit));
      return this;
   }
}
