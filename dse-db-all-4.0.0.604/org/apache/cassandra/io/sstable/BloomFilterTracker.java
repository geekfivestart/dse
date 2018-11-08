package org.apache.cassandra.io.sstable;

import java.util.concurrent.atomic.AtomicLong;

public class BloomFilterTracker {
   private final AtomicLong falsePositiveCount = new AtomicLong(0L);
   private final AtomicLong truePositiveCount = new AtomicLong(0L);
   private long lastFalsePositiveCount = 0L;
   private long lastTruePositiveCount = 0L;

   public BloomFilterTracker() {
   }

   public void addFalsePositive() {
      this.falsePositiveCount.incrementAndGet();
   }

   public void addTruePositive() {
      this.truePositiveCount.incrementAndGet();
   }

   public long getFalsePositiveCount() {
      return this.falsePositiveCount.get();
   }

   public long getRecentFalsePositiveCount() {
      long fpc = this.getFalsePositiveCount();

      long var3;
      try {
         var3 = fpc - this.lastFalsePositiveCount;
      } finally {
         this.lastFalsePositiveCount = fpc;
      }

      return var3;
   }

   public long getTruePositiveCount() {
      return this.truePositiveCount.get();
   }

   public long getRecentTruePositiveCount() {
      long tpc = this.getTruePositiveCount();

      long var3;
      try {
         var3 = tpc - this.lastTruePositiveCount;
      } finally {
         this.lastTruePositiveCount = tpc;
      }

      return var3;
   }
}
