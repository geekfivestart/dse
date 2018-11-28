package org.apache.cassandra.utils.memory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.Timer;

public abstract class MemtablePool {
   final MemtableCleanerThread<?> cleaner;
   public final MemtablePool.SubPool onHeap;
   public final MemtablePool.SubPool offHeap;
   public final Timer blockedOnAllocating;
   public AtomicReference<CompletableFuture<Void>> releaseFuture = new AtomicReference(new CompletableFuture());
   private static final AtomicLongFieldUpdater<MemtablePool.SubPool> reclaimingUpdater = AtomicLongFieldUpdater.newUpdater(MemtablePool.SubPool.class, "reclaiming");
   private static final AtomicLongFieldUpdater<MemtablePool.SubPool> allocatedUpdater = AtomicLongFieldUpdater.newUpdater(MemtablePool.SubPool.class, "allocated");
   private static final AtomicLongFieldUpdater<MemtablePool.SubPool> nextCleanUpdater = AtomicLongFieldUpdater.newUpdater(MemtablePool.SubPool.class, "nextClean");

   MemtablePool(long maxOnHeapMemory, long maxOffHeapMemory, double cleanThreshold, Runnable cleaner) {
      this.onHeap = this.getSubPool(maxOnHeapMemory, cleanThreshold);
      this.offHeap = this.getSubPool(maxOffHeapMemory, cleanThreshold);
      this.cleaner = this.getCleaner(cleaner);
      this.blockedOnAllocating = CassandraMetricsRegistry.Metrics.timer((new DefaultNameFactory("MemtablePool")).createMetricName("BlockedOnAllocation"));
      if(this.cleaner != null) {
         this.cleaner.start();
      }

   }

   MemtablePool.SubPool getSubPool(long limit, double cleanThreshold) {
      return new MemtablePool.SubPool(limit, cleanThreshold);
   }

   MemtableCleanerThread<?> getCleaner(Runnable cleaner) {
      return cleaner == null?null:new MemtableCleanerThread(this, cleaner);
   }

   public abstract MemtableAllocator newAllocator(int var1);

   public class SubPool {
      public final long limit;
      public final double cleanThreshold;
      volatile long allocated;
      volatile long reclaiming;
      volatile long nextClean;

      public SubPool(long limit, double cleanThreshold) {
         this.limit = limit;
         this.cleanThreshold = cleanThreshold;
      }

      boolean needsCleaning() {
         return this.used() > this.nextClean && this.updateNextClean();
      }

      void maybeClean() {
         if(this.needsCleaning() && MemtablePool.this.cleaner != null) {
            MemtablePool.this.cleaner.trigger();
         }

      }

      private boolean updateNextClean() {
         long current;
         long next;
         do {
            current = this.nextClean;
            long reclaiming = this.reclaiming;
            next = reclaiming + (long)((double)this.limit * this.cleanThreshold);
         } while(current != next && !MemtablePool.nextCleanUpdater.compareAndSet(this, current, next));

         return this.used() > next;
      }

      private void adjustAllocated(long size) {
         MemtablePool.allocatedUpdater.addAndGet(this, size);
      }

      void allocated(long size) {
         assert size >= 0L;

         if(size != 0L) {
            this.adjustAllocated(size);
            this.maybeClean();
         }
      }

      void released(long size) {
         assert size >= 0L;

         this.adjustAllocated(-size);
         CompletableFuture<Void> future = (CompletableFuture)MemtablePool.this.releaseFuture.getAndSet(new CompletableFuture());
         future.complete(null);
      }

      void reclaiming(long size) {
         if(size != 0L) {
            MemtablePool.reclaimingUpdater.addAndGet(this, size);
         }
      }

      void reclaimed(long size) {
         if(size != 0L) {
            MemtablePool.reclaimingUpdater.addAndGet(this, -size);
            if(this.updateNextClean() && MemtablePool.this.cleaner != null) {
               MemtablePool.this.cleaner.trigger();
            }

         }
      }

      public long used() {
         return this.allocated;
      }

      public float reclaimingRatio() {
         float r = (float)this.reclaiming / (float)this.limit;
         return Float.isNaN(r)?0.0F:r;
      }

      public float usedRatio() {
         float r = (float)this.allocated / (float)this.limit;
         return Float.isNaN(r)?0.0F:r;
      }

      public MemtableAllocator.SubAllocator newAllocator() {
         return new MemtableAllocator.SubAllocator(this);
      }

      public boolean belowLimit() {
         return this.used() <= this.limit;
      }

      public CompletableFuture<Void> releaseFuture() {
         return (CompletableFuture)MemtablePool.this.releaseFuture.get();
      }

      public Timer.Context blockedTimerContext() {
         return MemtablePool.this.blockedOnAllocating.timer();
      }
   }
}
