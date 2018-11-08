package org.apache.cassandra.utils.memory;

import com.google.common.annotations.VisibleForTesting;
import io.netty.util.concurrent.FastThreadLocal;
import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.BufferPoolMetrics;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.UnsafeByteBufferAccess;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferPool {
   public static final int CHUNK_SIZE = 65536;
   @VisibleForTesting
   public static long MEMORY_USAGE_THRESHOLD = (long)DatabaseDescriptor.getFileCacheSizeInMB() * 1024L * 1024L;
   private static final boolean ALLOCATE_ON_HEAP_WHEN_EXAHUSTED = false;
   @VisibleForTesting
   public static boolean DISABLED = Boolean.parseBoolean(System.getProperty("cassandra.test.disable_buffer_pool", "false"));
   @VisibleForTesting
   public static boolean DEBUG = false;
   private static final Logger logger = LoggerFactory.getLogger(BufferPool.class);
   private static final NoSpamLogger noSpamLogger;
   private static final ByteBuffer EMPTY_BUFFER;
   private static final BufferPool.GlobalPool globalPool;
   private static final BufferPoolMetrics metrics;
   private final FastThreadLocal<BufferPool.LocalPool> localPool = new FastThreadLocal<BufferPool.LocalPool>() {
      protected BufferPool.LocalPool initialValue() {
         return new BufferPool.LocalPool(null);
      }
   };
   private static final ConcurrentLinkedQueue<BufferPool.LocalPoolRef> localPoolReferences;
   private static final ReferenceQueue<Object> localPoolRefQueue;
   private static final ExecutorService EXEC;

   public BufferPool() {
   }

   public ByteBuffer get(int size) {
      return DISABLED?allocate(size, false):this.takeFromPool(size, false);
   }

   public ByteBuffer get(int size, BufferType bufferType) {
      boolean direct = bufferType != BufferType.ON_HEAP;
      return !DISABLED && direct?this.takeFromPool(size, !direct):allocate(size, !direct);
   }

   public ByteBuffer tryGet(int size) {
      return DISABLED?allocate(size, false):this.maybeTakeFromPool(size, false);
   }

   private static ByteBuffer allocate(int size, boolean onHeap) {
      return onHeap?ByteBuffer.allocate(size):BufferType.OFF_HEAP_ALIGNED.allocate(size);
   }

   private ByteBuffer takeFromPool(int size, boolean allocateOnHeapWhenExhausted) {
      ByteBuffer ret = this.maybeTakeFromPool(size, allocateOnHeapWhenExhausted);
      if(ret != null) {
         return ret;
      } else {
         if(logger.isTraceEnabled()) {
            logger.trace("Requested buffer size {} has been allocated directly due to lack of capacity", FBUtilities.prettyPrintMemory((long)size));
         }

         ByteBuffer buf = ((BufferPool.LocalPool)this.localPool.get()).allocate(size, allocateOnHeapWhenExhausted);
         globalPool.activeMemoryUsage.addAndGet((long)buf.capacity());
         return buf;
      }
   }

   private ByteBuffer maybeTakeFromPool(int size, boolean allocateOnHeapWhenExhausted) {
      if(size < 0) {
         throw new IllegalArgumentException("Size must be positive (" + size + ")");
      } else if(size == 0) {
         return EMPTY_BUFFER;
      } else {
         ByteBuffer buf;
         if(size > 65536) {
            if(logger.isTraceEnabled()) {
               logger.trace("Requested buffer size {} is bigger than {}, allocating directly", FBUtilities.prettyPrintMemory((long)size), FBUtilities.prettyPrintMemory(65536L));
            }

            buf = ((BufferPool.LocalPool)this.localPool.get()).allocate(size, allocateOnHeapWhenExhausted);
         } else {
            buf = ((BufferPool.LocalPool)this.localPool.get()).get(size);
         }

         if(buf != null) {
            globalPool.activeMemoryUsage.addAndGet((long)buf.capacity());
         }

         return buf;
      }
   }

   public void put(ByteBuffer buffer) {
      if(!DISABLED && !buffer.hasArray()) {
         ((BufferPool.LocalPool)this.localPool.get()).put(buffer);
      }

   }

   @VisibleForTesting
   void reset() {
      ((BufferPool.LocalPool)this.localPool.get()).reset();
      globalPool.reset();
   }

   @VisibleForTesting
   BufferPool.Chunk currentChunk() {
      return ((BufferPool.LocalPool)this.localPool.get()).chunks[0];
   }

   @VisibleForTesting
   int numChunks() {
      int ret = 0;
      BufferPool.Chunk[] var2 = ((BufferPool.LocalPool)this.localPool.get()).chunks;
      int var3 = var2.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         BufferPool.Chunk chunk = var2[var4];
         if(chunk != null) {
            ++ret;
         }
      }

      return ret;
   }

   @VisibleForTesting
   void assertAllRecycled() {
      globalPool.debug.check();
   }

   public static long sizeInBytes() {
      return globalPool.sizeInBytes();
   }

   public static long usedSizeInBytes() {
      return globalPool.usedSizeInBytes();
   }

   public static long sizeInBytesOverLimit() {
      return globalPool.sizeInBytesOverLimit();
   }

   @VisibleForTesting
   public static int roundUpNormal(int size) {
      return roundUp(size, 1024);
   }

   private static int roundUp(int size, int unit) {
      int mask = unit - 1;
      return size + mask & ~mask;
   }

   static {
      noSpamLogger = NoSpamLogger.getLogger(logger, 15L, TimeUnit.MINUTES);
      EMPTY_BUFFER = ByteBuffer.allocateDirect(0);
      globalPool = new BufferPool.GlobalPool(null);
      metrics = new BufferPoolMetrics();
      localPoolReferences = new ConcurrentLinkedQueue();
      localPoolRefQueue = new ReferenceQueue();
      EXEC = Executors.newFixedThreadPool(1, new NamedThreadFactory("LocalPool-Cleaner"));
      EXEC.execute(new Runnable() {
         public void run() {
            try {
               while(true) {
                  Object obj = BufferPool.localPoolRefQueue.remove();
                  if(obj instanceof BufferPool.LocalPoolRef) {
                     ((BufferPool.LocalPoolRef)obj).release();
                     BufferPool.localPoolReferences.remove(obj);
                  }
               }
            } catch (InterruptedException var5) {
               ;
            } finally {
               BufferPool.EXEC.execute(this);
            }

         }
      });
   }

   static final class Chunk {
      private final ByteBuffer slab;
      private final long baseAddress;
      private final int shift;
      private volatile long freeSlots;
      private static final AtomicLongFieldUpdater<BufferPool.Chunk> freeSlotsUpdater = AtomicLongFieldUpdater.newUpdater(BufferPool.Chunk.class, "freeSlots");
      private volatile BufferPool.LocalPool owner;
      private long lastRecycled;
      private final BufferPool.Chunk original;

      Chunk(BufferPool.Chunk recycle) {
         assert recycle.freeSlots == 0L;

         this.slab = recycle.slab;
         this.baseAddress = recycle.baseAddress;
         this.shift = recycle.shift;
         this.freeSlots = -1L;
         this.original = recycle.original;
         if(BufferPool.DEBUG) {
            BufferPool.globalPool.debug.recycle(this.original);
         }

      }

      Chunk(ByteBuffer slab) {
         assert !slab.hasArray();

         this.slab = slab;
         this.baseAddress = UnsafeByteBufferAccess.getAddress(slab);
         this.shift = 31 & Integer.numberOfTrailingZeros(slab.capacity() / 64);
         this.freeSlots = slab.capacity() == 0?0L:-1L;
         this.original = BufferPool.DEBUG?this:null;
      }

      void acquire(BufferPool.LocalPool owner) {
         assert this.owner == null;

         this.owner = owner;
      }

      void release() {
         this.owner = null;
         this.tryRecycle();
      }

      void tryRecycle() {
         assert this.owner == null;

         if(this.isFree() && freeSlotsUpdater.compareAndSet(this, -1L, 0L)) {
            this.recycle();
         }

      }

      void recycle() {
         assert this.freeSlots == 0L;

         BufferPool.globalPool.recycle(new BufferPool.Chunk(this));
      }

      static BufferPool.Chunk getParentChunk(ByteBuffer buffer) {
         Object attachment = UnsafeByteBufferAccess.getAttachment(buffer);
         return attachment instanceof BufferPool.Chunk?(BufferPool.Chunk)attachment:(attachment instanceof Ref?(BufferPool.Chunk)((Ref)attachment).get():null);
      }

      ByteBuffer setAttachment(ByteBuffer buffer) {
         if(Ref.DEBUG_ENABLED) {
            UnsafeByteBufferAccess.setAttachment(buffer, new Ref(this, (RefCounted.Tidy)null));
         } else {
            UnsafeByteBufferAccess.setAttachment(buffer, this);
         }

         return buffer;
      }

      boolean releaseAttachment(ByteBuffer buffer) {
         Object attachment = UnsafeByteBufferAccess.getAttachment(buffer);
         if(attachment == null) {
            return false;
         } else {
            if(attachment instanceof Ref) {
               ((Ref)attachment).release();
            }

            return true;
         }
      }

      @VisibleForTesting
      void reset() {
         BufferPool.Chunk parent = getParentChunk(this.slab);
         if(parent != null) {
            parent.free(this.slab, false);
         } else {
            FileUtils.clean(this.slab, true);
         }

      }

      @VisibleForTesting
      long setFreeSlots(long val) {
         long ret = this.freeSlots;
         this.freeSlots = val;
         return ret;
      }

      int capacity() {
         return 64 << this.shift;
      }

      final int unit() {
         return 1 << this.shift;
      }

      final boolean isFree() {
         return this.freeSlots == -1L;
      }

      int free() {
         return Long.bitCount(this.freeSlots) * this.unit();
      }

      ByteBuffer get(int size) {
         int slotCount = size - 1 + this.unit() >>> this.shift;
         if(slotCount > 64) {
            return null;
         } else {
            long slotBits = -1L >>> 64 - slotCount;
            long searchMask = 1229782938247303441L;
            searchMask *= 15L >>> (slotCount - 1 & 3);
            searchMask &= -1L >>> slotCount - 1;

            long cur;
            int index;
            long candidate;
            do {
               cur = this.freeSlots;
               index = Long.numberOfTrailingZeros(cur & searchMask);
               if(index == 64) {
                  return null;
               }

               searchMask ^= 1L << index;
               candidate = slotBits << index;
            } while((candidate & cur) != candidate);

            do {
               if(freeSlotsUpdater.compareAndSet(this, cur, cur & ~candidate)) {
                  return this.get(index << this.shift, size);
               }

               cur = this.freeSlots;
            } while($assertionsDisabled || (candidate & cur) == candidate);

            throw new AssertionError();
         }
      }

      private ByteBuffer get(int offset, int size) {
         this.slab.limit(offset + size);
         this.slab.position(offset);
         return this.setAttachment(this.slab.slice());
      }

      int roundUp(int v) {
         return BufferPool.roundUp(v, this.unit());
      }

      long free(ByteBuffer buffer, boolean tryRelease) {
         if(!this.releaseAttachment(buffer)) {
            return 1L;
         } else {
            long address = UnsafeByteBufferAccess.getAddress(buffer);

            assert address >= this.baseAddress & address <= this.baseAddress + (long)this.capacity();

            int position = (int)(address - this.baseAddress);
            int size = this.roundUp(buffer.capacity());
            position >>= this.shift;
            int slotCount = size >> this.shift;
            long slotBits = (1L << slotCount) - 1L;
            long shiftedSlotBits = slotBits << position;
            if(slotCount == 64) {
               assert size == this.capacity();

               assert position == 0;

               shiftedSlotBits = -1L;
            }

            long next;
            long cur;
            do {
               cur = this.freeSlots;
               next = cur | shiftedSlotBits;

               assert next == (cur ^ shiftedSlotBits);

               if(tryRelease && next == -1L) {
                  next = 0L;
               }
            } while(!freeSlotsUpdater.compareAndSet(this, cur, next));

            return next;
         }
      }

      public String toString() {
         return String.format("[slab %s, slots bitmap %s, capacity %d, free %d]", new Object[]{this.slab, Long.toBinaryString(this.freeSlots), Integer.valueOf(this.capacity()), Integer.valueOf(this.free())});
      }
   }

   private static final class LocalPoolRef extends PhantomReference<BufferPool.LocalPool> {
      private final BufferPool.Chunk[] chunks;

      public LocalPoolRef(BufferPool.LocalPool localPool, ReferenceQueue<? super BufferPool.LocalPool> q) {
         super(localPool, q);
         this.chunks = localPool.chunks;
      }

      public void release() {
         for(int i = 0; i < this.chunks.length; ++i) {
            if(this.chunks[i] != null) {
               this.chunks[i].release();
               this.chunks[i] = null;
            }
         }

      }
   }

   static final class LocalPool {
      private final BufferPool.Chunk[] chunks;
      private byte chunkCount;

      private LocalPool() {
         this.chunks = new BufferPool.Chunk[3];
         this.chunkCount = 0;
         BufferPool.localPoolReferences.add(new BufferPool.LocalPoolRef(this, BufferPool.localPoolRefQueue));
      }

      private BufferPool.Chunk addChunkFromGlobalPool() {
         BufferPool.Chunk chunk = BufferPool.globalPool.get();
         if(chunk == null) {
            return null;
         } else {
            this.addChunk(chunk);
            return chunk;
         }
      }

      private void addChunk(BufferPool.Chunk chunk) {
         chunk.acquire(this);
         if(this.chunkCount < 3) {
            this.chunks[this.chunkCount++] = chunk;
         } else {
            int smallestChunkIdx = 0;
            if(this.chunks[1].free() < this.chunks[0].free()) {
               smallestChunkIdx = 1;
            }

            if(this.chunks[2].free() < this.chunks[smallestChunkIdx].free()) {
               smallestChunkIdx = 2;
            }

            this.chunks[smallestChunkIdx].release();
            if(smallestChunkIdx != 2) {
               this.chunks[smallestChunkIdx] = this.chunks[2];
            }

            this.chunks[2] = chunk;
         }
      }

      public ByteBuffer get(int size) {
         BufferPool.Chunk[] var2 = this.chunks;
         int var3 = var2.length;

         for(int var4 = 0; var4 < var3; ++var4) {
            BufferPool.Chunk chunk = var2[var4];
            if(chunk == null) {
               break;
            }

            ByteBuffer buffer = chunk.get(size);
            if(buffer != null) {
               return buffer;
            }
         }

         BufferPool.Chunk chunk = this.addChunkFromGlobalPool();
         return chunk != null?chunk.get(size):null;
      }

      private ByteBuffer allocate(int size, boolean onHeap) {
         BufferPool.metrics.misses.mark();
         return BufferPool.allocate(size, onHeap);
      }

      public void put(ByteBuffer buffer) {
         BufferPool.globalPool.activeMemoryUsage.addAndGet((long)(-buffer.capacity()));
         BufferPool.Chunk chunk = BufferPool.Chunk.getParentChunk(buffer);
         if(chunk == null) {
            FileUtils.clean(buffer, true);
         } else {
            BufferPool.LocalPool owner = chunk.owner;
            long free = chunk.free(buffer, owner == null | owner == this);
            if(free == 0L) {
               chunk.recycle();
               if(owner == this) {
                  this.removeFromLocalQueue(chunk);
               }
            } else if(free == -1L && owner != this && chunk.owner == null) {
               chunk.tryRecycle();
            }

         }
      }

      private void removeFromLocalQueue(BufferPool.Chunk chunk) {
         if(this.chunks[0] == chunk) {
            this.chunks[0] = this.chunks[1];
            this.chunks[1] = this.chunks[2];
         } else if(this.chunks[1] == chunk) {
            this.chunks[1] = this.chunks[2];
         } else {
            assert this.chunks[2] == chunk;
         }

         this.chunks[2] = null;
         --this.chunkCount;
      }

      @VisibleForTesting
      void reset() {
         this.chunkCount = 0;

         for(int i = 0; i < this.chunks.length; ++i) {
            if(this.chunks[i] != null) {
               this.chunks[i].owner = null;
               this.chunks[i].freeSlots = 0L;
               this.chunks[i].recycle();
               this.chunks[i] = null;
            }
         }

      }
   }

   static final class GlobalPool {
      static final int MACRO_CHUNK_SIZE = 1048576;
      private final BufferPool.Debug debug;
      private final Queue<BufferPool.Chunk> macroChunks;
      private final Queue<BufferPool.Chunk> chunks;
      private final AtomicLong memoryUsage;
      private final AtomicLong activeMemoryUsage;

      private GlobalPool() {
         this.debug = new BufferPool.Debug();
         this.macroChunks = new ConcurrentLinkedQueue();
         this.chunks = new ConcurrentLinkedQueue();
         this.memoryUsage = new AtomicLong();
         this.activeMemoryUsage = new AtomicLong();
      }

      public BufferPool.Chunk get() {
         do {
            BufferPool.Chunk chunk = (BufferPool.Chunk)this.chunks.poll();
            if(chunk != null) {
               return chunk;
            }
         } while(this.allocateMoreChunks());

         return (BufferPool.Chunk)this.chunks.poll();
      }

      private boolean allocateMoreChunks() {
         long cur;
         do {
            cur = this.memoryUsage.get();
            if(cur + 1048576L > BufferPool.MEMORY_USAGE_THRESHOLD) {
               BufferPool.noSpamLogger.info("Maximum memory usage reached ({}), cannot allocate chunk of {}", new Object[]{Long.valueOf(BufferPool.MEMORY_USAGE_THRESHOLD), Integer.valueOf(1048576)});
               return false;
            }
         } while(!this.memoryUsage.compareAndSet(cur, cur + 1048576L));

         BufferPool.Chunk chunk;
         try {
            chunk = new BufferPool.Chunk(BufferType.OFF_HEAP_ALIGNED.allocate(1048576));
         } catch (OutOfMemoryError var4) {
            BufferPool.noSpamLogger.error("Buffer pool failed to allocate chunk of {}, current size {} ({}). Attempting to continue; buffers will be allocated in on-heap memory which can degrade performance. Make sure direct memory size (-XX:MaxDirectMemorySize) is large enough to accommodate off-heap memtables and caches.", new Object[]{Integer.valueOf(1048576), Long.valueOf(this.sizeInBytes()), var4.toString()});
            return false;
         }

         chunk.acquire((BufferPool.LocalPool)null);
         this.macroChunks.add(chunk);

         for(int i = 0; i < 1048576; i += 65536) {
            BufferPool.Chunk add = new BufferPool.Chunk(chunk.get(65536));
            this.chunks.add(add);
            if(BufferPool.DEBUG) {
               this.debug.register(add);
            }
         }

         return true;
      }

      public void recycle(BufferPool.Chunk chunk) {
         this.chunks.add(chunk);
      }

      public long sizeInBytes() {
         return this.memoryUsage.get();
      }

      public long usedSizeInBytes() {
         return this.activeMemoryUsage.get();
      }

      public long sizeInBytesOverLimit() {
         return Math.max(0L, this.activeMemoryUsage.get() - this.memoryUsage.get());
      }

      @VisibleForTesting
      void reset() {
         while(!this.chunks.isEmpty()) {
            ((BufferPool.Chunk)this.chunks.poll()).reset();
         }

         while(!this.macroChunks.isEmpty()) {
            ((BufferPool.Chunk)this.macroChunks.poll()).reset();
         }

         this.memoryUsage.set(0L);
         this.activeMemoryUsage.set(0L);
      }

      static {
         assert Integer.bitCount(65536) == 1;

         assert Integer.bitCount(1048576) == 1;

         if(BufferPool.DISABLED) {
            BufferPool.logger.info("Global buffer pool is disabled, allocating {}", "off heap");
         } else {
            BufferPool.logger.info("Global buffer pool is enabled, when pool is exhausted (max is {}) it will allocate {}", FBUtilities.prettyPrintMemory(BufferPool.MEMORY_USAGE_THRESHOLD), "off heap");
         }

      }
   }

   static final class Debug {
      long recycleRound = 1L;
      final Queue<BufferPool.Chunk> allChunks = new ConcurrentLinkedQueue();

      Debug() {
      }

      void register(BufferPool.Chunk chunk) {
         this.allChunks.add(chunk);
      }

      void recycle(BufferPool.Chunk chunk) {
         chunk.lastRecycled = this.recycleRound;
      }

      void check() {
         Iterator var1 = this.allChunks.iterator();

         BufferPool.Chunk chunk;
         do {
            if(!var1.hasNext()) {
               ++this.recycleRound;
               return;
            }

            chunk = (BufferPool.Chunk)var1.next();
         } while($assertionsDisabled || chunk.lastRecycled == this.recycleRound);

         throw new AssertionError();
      }
   }
}
