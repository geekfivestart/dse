package com.datastax.bdp.util;

import com.datastax.bdp.system.TimeSource;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class RetrySetup {
   private final TimeSource timeSource;
   private final Duration timeout;
   private final Duration interval;

   public RetrySetup(TimeSource timeSource, Duration timeout, Duration interval) {
      this.timeSource = timeSource;
      this.timeout = timeout;
      this.interval = interval;
   }

   public RetrySetup.RetrySchedule fromNow() {
      return new RetrySetup.RetrySchedule(this.timeSource, this.timeSource.currentTimeMillis() + this.timeout.toMillis(), this.interval.toMillis(), null);
   }

   public RetrySetup.RetrySchedule fromNow(final Object refToWaitOn) {
      return new RetrySetup.RetrySchedule(this.timeSource, this.timeSource.currentTimeMillis() + this.timeout.toMillis(), this.interval.toMillis()) {
         public void waitForNextTry() {
            try {
               refToWaitOn.wait(this.interval);
            } catch (InterruptedException var2) {
               Thread.currentThread().interrupt();
            }

         }
      };
   }

   public static class RetrySchedule {
      private final TimeSource timeSource;
      public final long deadline;
      public final long interval;

      private RetrySchedule(TimeSource timeSource, long deadline, long interval) {
         this.timeSource = timeSource;
         this.deadline = deadline;
         this.interval = interval;
      }

      public boolean hasMoreTries() {
         return this.timeSource.currentTimeMillis() <= this.deadline;
      }

      public void waitForNextTry() {
         this.timeSource.sleepUninterruptibly(this.interval, TimeUnit.MILLISECONDS);
      }
   }
}
