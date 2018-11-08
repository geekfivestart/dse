package org.apache.cassandra.io.compress;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BufferRecycler {
   private static final Logger logger = LoggerFactory.getLogger(BufferRecycler.class);
   private final ConcurrentMap<Thread, CopyOnWriteArrayList<BufferRecycler.BufferHolder>> spareBuffers = new ConcurrentHashMap();
   public static final int BUFFER_SEARCH_ITERATIONS = 32;
   public static final int BUFFER_SEARCH_SIZE_WINDOW = 4;
   public static final float OVERALLOCATE = 1.1F;
   public static final int BUFFER_EXPIRING_THREAD_INTERVAL = 500;
   public static final int DEFAULT_TTL = 100;
   public static final BufferRecycler instance = new BufferRecycler();
   private final BufferRecycler.BufferExpiringThread bufferExpiringThread = new BufferRecycler.BufferExpiringThread();

   public BufferRecycler() {
      this.bufferExpiringThread.start();
   }

   public ByteBuffer allocate(int minCapacity, BufferType type) {
      BufferRecycler.BufferAllocator allocator = new BufferRecycler.BufferAllocator(minCapacity, type);
      ByteBuffer buffer = allocator.allocateFromLocalThread();
      if(buffer == null) {
         buffer = allocator.allocateFromOtherThreads();
      }

      if(buffer == null) {
         buffer = type.allocate((int)((float)minCapacity * 1.1F));
      }

      return buffer;
   }

   public void recycle(ByteBuffer buffer) {
      this.recycle(buffer, 100);
   }

   public void recycle(ByteBuffer buffer, int ttl) {
      if(buffer != null) {
         buffer.rewind();
         buffer.limit(buffer.capacity());
         Thread thread = Thread.currentThread();
         Collection<BufferRecycler.BufferHolder> threadBuffers = this.getBuffers(thread);
         threadBuffers.add(new BufferRecycler.BufferHolder(buffer, ttl));
      }

   }

   public void shutdown() {
      this.bufferExpiringThread.terminate();
   }

   private CopyOnWriteArrayList<BufferRecycler.BufferHolder> getBuffers(Thread thread) {
      CopyOnWriteArrayList<BufferRecycler.BufferHolder> threadBuffers = (CopyOnWriteArrayList)this.spareBuffers.get(thread);
      if(threadBuffers == null) {
         threadBuffers = new CopyOnWriteArrayList();
         CopyOnWriteArrayList<BufferRecycler.BufferHolder> prevBuffers = (CopyOnWriteArrayList)this.spareBuffers.putIfAbsent(thread, threadBuffers);
         if(prevBuffers != null) {
            threadBuffers = prevBuffers;
         }
      }

      return threadBuffers;
   }

   private final class BufferHolder {
      final ByteBuffer buffer;
      final int capacity;
      final long created;
      final int ttl;

      private BufferHolder(ByteBuffer buffer, int ttl) {
         this.buffer = buffer;
         this.capacity = buffer.capacity();
         this.created = System.currentTimeMillis();
         this.ttl = ttl;
      }

      public boolean isExpired() {
         return System.currentTimeMillis() - this.created > (long)this.ttl;
      }

      public BufferType getType() {
         return this.buffer.isDirect()?BufferType.OFF_HEAP:BufferType.ON_HEAP;
      }
   }

   private class BufferExpiringThread extends Thread {
      public volatile boolean terminated = false;

      public BufferExpiringThread() {
         this.setDaemon(true);
         this.setName("buffer recycler @" + this.hashCode());
      }

      public synchronized void run() {
         while(!this.terminated) {
            try {
               Iterator var1 = BufferRecycler.this.spareBuffers.entrySet().iterator();

               while(var1.hasNext()) {
                  Entry<Thread, CopyOnWriteArrayList<BufferRecycler.BufferHolder>> e = (Entry)var1.next();
                  if(!((Thread)e.getKey()).isAlive()) {
                     BufferRecycler.this.spareBuffers.remove(e.getKey());
                  } else {
                     this.removeExpiredBuffers((CopyOnWriteArrayList)e.getValue());
                  }
               }

               this.wait(500L);
            } catch (InterruptedException var3) {
               BufferRecycler.logger.info("Buffer recycler thread interrupted");
            }
         }

      }

      public synchronized void terminate() {
         this.terminated = true;
         this.notify();
      }

      private void removeExpiredBuffers(CopyOnWriteArrayList<BufferRecycler.BufferHolder> buffers) {
         ArrayList<BufferRecycler.BufferHolder> expiredBuffers = new ArrayList();
         Iterator var3 = buffers.iterator();

         while(var3.hasNext()) {
            BufferRecycler.BufferHolder bh = (BufferRecycler.BufferHolder)var3.next();
            if(bh.isExpired()) {
               expiredBuffers.add(bh);
            }
         }

         buffers.removeAll(expiredBuffers);
      }
   }

   private class BufferAllocator {
      private final int minCapacity;
      private final BufferType type;

      private BufferAllocator(int minCapacity, BufferType type) {
         this.minCapacity = minCapacity;
         this.type = type;
      }

      public ByteBuffer allocateFromLocalThread() {
         return this.allocateFromThread(Thread.currentThread(), true);
      }

      public ByteBuffer allocateFromOtherThreads() {
         Thread currentThread = Thread.currentThread();
         int bufferCounter = 0;
         Iterator var3 = BufferRecycler.this.spareBuffers.entrySet().iterator();

         while(var3.hasNext()) {
            Entry<Thread, CopyOnWriteArrayList<BufferRecycler.BufferHolder>> e = (Entry)var3.next();
            if(e.getKey() != currentThread) {
               bufferCounter += ((CopyOnWriteArrayList)e.getValue()).size();
               ByteBuffer buffer = this.allocateFromThread((Thread)e.getKey(), false);
               if(buffer != null) {
                  return buffer;
               }
            }

            if(bufferCounter > 32) {
               break;
            }
         }

         return null;
      }

      private ByteBuffer allocateFromThread(Thread thread, boolean allowNonExpired) {
         CopyOnWriteArrayList buffers = BufferRecycler.this.getBuffers(thread);

         BufferRecycler.BufferHolder bh;
         do {
            bh = this.findBuffer(buffers, allowNonExpired);
         } while(bh != null && !buffers.remove(bh));

         return bh == null?null:bh.buffer;
      }

      private BufferRecycler.BufferHolder findBuffer(Collection<BufferRecycler.BufferHolder> buffers, boolean allowNonExpired) {
         Iterator var3 = buffers.iterator();

         BufferRecycler.BufferHolder bh;
         do {
            do {
               do {
                  do {
                     if(!var3.hasNext()) {
                        return null;
                     }

                     bh = (BufferRecycler.BufferHolder)var3.next();
                  } while(bh.capacity < this.minCapacity);
               } while(bh.capacity > this.minCapacity * 4);
            } while(bh.getType() != this.type);
         } while(!allowNonExpired && !bh.isExpired());

         return bh;
      }
   }
}
