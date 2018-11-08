package org.apache.cassandra.utils.memory;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.NativeClustering;
import org.apache.cassandra.db.NativeDecoratedKey;
import org.apache.cassandra.db.rows.ArrayBackedRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.NativeCell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.utils.UnsafeMemoryAccess;

public class NativeAllocator extends MemtableAllocator {
   private static final int MAX_REGION_SIZE = 1048576;
   private static final int MAX_CLONED_SIZE = 131072;
   private static final int MIN_REGION_SIZE = 8192;
   private NativeAllocator.Region currentRegion = null;
   private Collection<NativeAllocator.Region> regions = new LinkedList();
   private final int coreId;

   protected NativeAllocator(NativePool pool, int coreId) {
      super(pool.onHeap.newAllocator(), pool.offHeap.newAllocator());

      assert TPC.isValidCoreId(coreId) || coreId == -1;

      this.coreId = coreId;
   }

   public Row.Builder rowBuilder() {
      return new NativeAllocator.CloningRowBuilder(this);
   }

   public DecoratedKey clone(DecoratedKey key) {
      return new NativeDecoratedKey(key.getToken(), this, key.getKey());
   }

   public boolean onHeapOnly() {
      return false;
   }

   public long allocate(int size) {
      assert TPC.isOnCore(this.coreId) || this.coreId == -1;

      assert size >= 0;

      this.offHeap().allocated((long)size);
      if(size > 131072) {
         return this.allocateOversize(size);
      } else {
         NativeAllocator.Region region = this.currentRegion;
         long peer;
         if(region != null && (peer = region.allocate(size)) > 0L) {
            return peer;
         } else {
            peer = this.trySwapRegion(size).allocate(size);

            assert peer > 0L;

            return peer;
         }
      }
   }

   private NativeAllocator.Region trySwapRegion(int minSize) {
      NativeAllocator.Region current = this.currentRegion;
      int size;
      if(current == null) {
         size = 8192;
      } else {
         size = current.capacity * 2;
      }

      if(minSize > size) {
         size = Integer.highestOneBit(minSize) << 3;
      }

      size = Math.min(1048576, size);
      NativeAllocator.Region next = new NativeAllocator.Region(UnsafeMemoryAccess.allocate((long)size), size);
      this.currentRegion = next;
      this.regions.add(next);
      return next;
   }

   private long allocateOversize(int size) {
      NativeAllocator.Region region = new NativeAllocator.Region(UnsafeMemoryAccess.allocate((long)size), size);
      this.regions.add(region);
      long peer;
      if((peer = region.allocate(size)) == -1L) {
         throw new AssertionError();
      } else {
         return peer;
      }
   }

   public void setDiscarded() {
      Iterator var1 = this.regions.iterator();

      while(var1.hasNext()) {
         NativeAllocator.Region region = (NativeAllocator.Region)var1.next();
         UnsafeMemoryAccess.free(region.peer);
      }

      super.setDiscarded();
   }

   private static class Region {
      private final long peer;
      private final int capacity;
      private int nextFreeOffset;
      private int allocCount;

      private Region(long peer, int capacity) {
         this.nextFreeOffset = 0;
         this.allocCount = 0;
         this.peer = peer;
         this.capacity = capacity;
      }

      long allocate(int size) {
         int oldOffset = this.nextFreeOffset;
         if(oldOffset + size > this.capacity) {
            return -1L;
         } else {
            this.nextFreeOffset += size;
            ++this.allocCount;
            return this.peer + (long)oldOffset;
         }
      }

      public String toString() {
         return "Region@" + System.identityHashCode(this) + " allocs=" + this.allocCount + "waste=" + (this.capacity - this.nextFreeOffset);
      }
   }

   private static class CloningRowBuilder extends ArrayBackedRow.Builder {
      final NativeAllocator allocator;

      private CloningRowBuilder(NativeAllocator allocator) {
         super(true, -2147483648);
         this.allocator = allocator;
      }

      public void newRow(Clustering clustering) {
         if(clustering != Clustering.STATIC_CLUSTERING) {
            clustering = new NativeClustering(this.allocator, (Clustering)clustering);
         }

         super.newRow((Clustering)clustering);
      }

      public void addCell(Cell cell) {
         super.addCell(new NativeCell(this.allocator, cell));
      }
   }
}
