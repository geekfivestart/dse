package org.apache.cassandra.utils.memory;

public class NativePool extends MemtablePool {
   public NativePool(long maxOnHeapMemory, long maxOffHeapMemory, double cleanThreshold, Runnable cleaner) {
      super(maxOnHeapMemory, maxOffHeapMemory, cleanThreshold, cleaner);
   }

   public NativeAllocator newAllocator(int coreId) {
      return new NativeAllocator(this, coreId);
   }
}
