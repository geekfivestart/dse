package org.apache.cassandra.utils.memory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.utils.ByteBufferUtil;
import sun.nio.ch.DirectBuffer;

public class SlabAllocator extends MemtableBufferAllocator {
   private static final int MIN_REGION_SIZE = 8192;
   private static final int MAX_REGION_SIZE = 1048576;
   private static final int MAX_CLONED_SIZE = 131072;
   private SlabAllocator.Region currentRegion = null;
   private final Collection<SlabAllocator.Region> offHeapRegions = new LinkedList();
   private final boolean allocateOnHeapOnly;
   private final int coreId;

   SlabAllocator(MemtableAllocator.SubAllocator onHeap, MemtableAllocator.SubAllocator offHeap, boolean allocateOnHeapOnly, int coreId) {
      super(onHeap, offHeap);

      assert TPC.isValidCoreId(coreId) || coreId == -1;

      this.coreId = coreId;
      this.allocateOnHeapOnly = allocateOnHeapOnly;
   }

   public boolean onHeapOnly() {
      return this.allocateOnHeapOnly;
   }

   public ByteBuffer allocate(int size) {
      assert TPC.isOnCore(this.coreId) || this.coreId == -1;

      assert size >= 0;

      if(size == 0) {
         return ByteBufferUtil.EMPTY_BYTE_BUFFER;
      } else {
         (this.allocateOnHeapOnly?this.onHeap():this.offHeap()).allocated((long)size);
         if(size > 131072) {
            if(this.allocateOnHeapOnly) {
               return ByteBuffer.allocate(size);
            } else {
               SlabAllocator.Region region = new SlabAllocator.Region(ByteBuffer.allocateDirect(size));
               this.offHeapRegions.add(region);
               return region.allocate(size);
            }
         } else {
            if(this.currentRegion != null) {
               ByteBuffer cloned = this.currentRegion.allocate(size);
               if(cloned != null) {
                  return cloned;
               }
            }

            int regionSize = this.getRegionSize(size);

            assert regionSize >= size;

            SlabAllocator.Region next = new SlabAllocator.Region(this.allocateOnHeapOnly?ByteBuffer.allocate(regionSize):ByteBuffer.allocateDirect(regionSize));
            if(!this.allocateOnHeapOnly) {
               this.offHeapRegions.add(next);
            }

            this.currentRegion = next;
            ByteBuffer cloned = this.currentRegion.allocate(size);

            assert cloned != null;

            return cloned;
         }
      }
   }

   public int getRegionSize(int minSize) {
      SlabAllocator.Region current = this.currentRegion;
      int regionSize;
      if(current == null) {
         regionSize = 8192;
      } else {
         regionSize = current.data.capacity() * 2;
      }

      if(minSize > regionSize) {
         regionSize = Integer.highestOneBit(minSize) << 3;
      }

      return Math.min(1048576, regionSize);
   }

   public void setDiscarded() {
      Iterator var1 = this.offHeapRegions.iterator();

      while(var1.hasNext()) {
         SlabAllocator.Region region = (SlabAllocator.Region)var1.next();
         ((DirectBuffer)region.data).cleaner().clean();
      }

      super.setDiscarded();
   }

   private static class Region {
      private final ByteBuffer data;
      private int nextFreeOffset;
      private int allocCount;

      private Region(ByteBuffer buffer) {
         this.nextFreeOffset = 0;
         this.allocCount = 0;
         this.data = buffer;
      }

      public ByteBuffer allocate(int size) {
         int oldOffset = this.nextFreeOffset;
         if(oldOffset + size > this.data.capacity()) {
            return null;
         } else {
            this.nextFreeOffset += size;
            ++this.allocCount;
            return (ByteBuffer)this.data.duplicate().position(oldOffset).limit(oldOffset + size);
         }
      }

      public String toString() {
         return "Region@" + System.identityHashCode(this) + " allocs=" + this.allocCount + "waste=" + (this.data.capacity() - this.nextFreeOffset);
      }
   }
}
