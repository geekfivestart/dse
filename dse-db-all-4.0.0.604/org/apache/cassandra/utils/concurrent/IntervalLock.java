package org.apache.cassandra.utils.concurrent;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.cassandra.utils.TimeSource;

public class IntervalLock extends ReentrantReadWriteLock {
   private final AtomicLong lastAcquire = new AtomicLong();
   private final TimeSource timeSource;

   public IntervalLock(TimeSource timeSource) {
      this.timeSource = timeSource;
   }

   public boolean tryIntervalLock(long interval) {
      long now = this.timeSource.currentTimeMillis();
      boolean acquired = now - this.lastAcquire.get() >= interval && this.writeLock().tryLock();
      if(acquired) {
         this.lastAcquire.set(now);
      }

      return acquired;
   }

   public void releaseIntervalLock() {
      this.writeLock().unlock();
   }

   @VisibleForTesting
   public long getLastIntervalAcquire() {
      return this.lastAcquire.get();
   }
}
