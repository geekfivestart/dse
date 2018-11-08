package org.apache.cassandra.hints;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

final class HintsBufferPool {
   static final int MAX_ALLOCATED_BUFFERS = Integer.getInteger("cassandra.MAX_HINT_BUFFERS", 3).intValue();
   private volatile HintsBuffer currentBuffer;
   private final BlockingQueue<HintsBuffer> reserveBuffers = new LinkedBlockingQueue();
   private final int bufferSize;
   private final HintsBufferPool.FlushCallback flushCallback;
   private int allocatedBuffers = 0;

   HintsBufferPool(int bufferSize, HintsBufferPool.FlushCallback flushCallback) {
      this.bufferSize = bufferSize;
      this.flushCallback = flushCallback;
   }

   void write(Iterable<UUID> hostIds, Hint hint) {
      long hintSize = ((Hint.HintSerializer)Hint.serializers.get(HintsDescriptor.CURRENT_VERSION)).serializedSize(hint);
      HintsBuffer.Allocation allocation = this.allocate(Math.toIntExact(hintSize));
      Throwable var6 = null;

      try {
         allocation.write(hostIds, hint);
      } catch (Throwable var15) {
         var6 = var15;
         throw var15;
      } finally {
         if(allocation != null) {
            if(var6 != null) {
               try {
                  allocation.close();
               } catch (Throwable var14) {
                  var6.addSuppressed(var14);
               }
            } else {
               allocation.close();
            }
         }

      }

   }

   private HintsBuffer.Allocation allocate(int hintSize) {
      HintsBuffer current = this.currentBuffer();

      while(true) {
         HintsBuffer.Allocation allocation = current.allocate(hintSize);
         if(allocation != null) {
            return allocation;
         }

         if(this.switchCurrentBuffer(current)) {
            this.flushCallback.flush(current, this);
         }

         current = this.currentBuffer;
      }
   }

   void offer(HintsBuffer buffer) {
      if(!this.reserveBuffers.offer(buffer)) {
         throw new RuntimeException("Failed to store buffer");
      }
   }

   HintsBuffer currentBuffer() {
      if(this.currentBuffer == null) {
         this.initializeCurrentBuffer();
      }

      return this.currentBuffer;
   }

   private synchronized void initializeCurrentBuffer() {
      if(this.currentBuffer == null) {
         this.currentBuffer = this.createBuffer();
      }

   }

   private synchronized boolean switchCurrentBuffer(HintsBuffer previous) {
      if(this.currentBuffer != previous) {
         return false;
      } else {
         HintsBuffer buffer = (HintsBuffer)this.reserveBuffers.poll();
         if(buffer == null && this.allocatedBuffers >= MAX_ALLOCATED_BUFFERS) {
            try {
               buffer = (HintsBuffer)this.reserveBuffers.take();
            } catch (InterruptedException var4) {
               throw new RuntimeException(var4);
            }
         }

         this.currentBuffer = buffer == null?this.createBuffer():buffer;
         return true;
      }
   }

   private HintsBuffer createBuffer() {
      ++this.allocatedBuffers;
      return HintsBuffer.create(this.bufferSize);
   }

   interface FlushCallback {
      void flush(HintsBuffer var1, HintsBufferPool var2);
   }
}
