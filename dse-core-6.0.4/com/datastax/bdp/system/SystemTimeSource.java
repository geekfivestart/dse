package com.datastax.bdp.system;

import com.google.common.util.concurrent.Uninterruptibles;
import java.util.concurrent.TimeUnit;

public class SystemTimeSource implements TimeSource {
   public SystemTimeSource() {
   }

   public long currentTimeMillis() {
      return System.currentTimeMillis();
   }

   public long nanoTime() {
      return System.nanoTime();
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
