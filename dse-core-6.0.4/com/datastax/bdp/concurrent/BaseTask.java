package com.datastax.bdp.concurrent;

import com.datastax.bdp.system.TimeSource;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public abstract class BaseTask implements Task {
   private final CountDownLatch latch = new CountDownLatch(1);
   private final long timestamp;
   private volatile long epoch;

   public BaseTask(TimeSource timeSource) {
      this.timestamp = timeSource.nanoTime();
   }

   public void await() throws InterruptedException {
      this.latch.await();
   }

   public boolean await(long time, TimeUnit unit) throws InterruptedException {
      return this.latch.await(time, unit);
   }

   public void signal() {
      this.latch.countDown();
   }

   public long getEpoch() {
      return this.epoch;
   }

   public void setEpoch(long epoch) {
      this.epoch = epoch;
   }

   public long getTimestampNanos() {
      return this.timestamp;
   }
}
