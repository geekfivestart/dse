package org.apache.cassandra.utils.memory;

public class SlabPool extends MemtablePool {
   final boolean allocateOnHeap;

   public SlabPool(long maxOnHeapMemory, long maxOffHeapMemory, double cleanupThreshold, Runnable cleaner) {
      super(maxOnHeapMemory, maxOffHeapMemory, cleanupThreshold, cleaner);
      this.allocateOnHeap = maxOffHeapMemory == 0L;
   }

   public MemtableAllocator newAllocator(int coreId) {
      return new SlabAllocator(this.onHeap.newAllocator(), this.offHeap.newAllocator(), this.allocateOnHeap, coreId);
   }
}
