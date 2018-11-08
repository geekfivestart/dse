package org.apache.cassandra.utils.memory;

import java.nio.ByteBuffer;

public class HeapPool extends MemtablePool {
   public HeapPool(long maxOnHeapMemory, double cleanupThreshold, Runnable cleaner) {
      super(maxOnHeapMemory, 0L, cleanupThreshold, cleaner);
   }

   public MemtableAllocator newAllocator(int coreId) {
      return new HeapPool.Allocator(this);
   }

   private static class Allocator extends MemtableBufferAllocator {
      Allocator(HeapPool pool) {
         super(pool.onHeap.newAllocator(), pool.offHeap.newAllocator());
      }

      public boolean onHeapOnly() {
         return true;
      }

      public ByteBuffer allocate(int size) {
         super.onHeap().allocated((long)size);
         return ByteBuffer.allocate(size);
      }
   }
}
