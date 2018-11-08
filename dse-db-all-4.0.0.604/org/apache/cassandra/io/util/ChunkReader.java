package org.apache.cassandra.io.util;

import com.google.common.annotations.VisibleForTesting;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.cassandra.io.compress.BufferType;
import org.jctools.queues.MpmcArrayQueue;

public interface ChunkReader extends RebuffererFactory {
   MpmcArrayQueue<ChunkReader.BufferHandle> scratchBuffers = new MpmcArrayQueue(32767);
   Long memoryLimit = Long.valueOf(Long.getLong("dse.total_chunk_reader_buffer_limit_mb", 128L).longValue() * 1024L * 1024L);
   AtomicLong bufferSize = new AtomicLong();

   CompletableFuture<ByteBuffer> readChunk(long var1, ByteBuffer var3);

   int chunkSize();

   BufferType preferredBufferType();

   boolean isMmap();

   ChunkReader withChannel(AsynchronousChannelProxy var1);

   default ChunkReader.BufferHandle getScratchHandle() {
      ChunkReader.BufferHandle handle = (ChunkReader.BufferHandle)scratchBuffers.relaxedPoll();
      return handle == null?new ChunkReader.BufferHandle():handle;
   }

   @VisibleForTesting
   static default ChunkReader simple(AsynchronousChannelProxy channel, long fileLength, BufferType bufferType, int bufferSize) {
      return new SimpleChunkReader(channel, fileLength, bufferType, bufferSize);
   }

   public static class BufferHandle {
      private ByteBuffer alignedBuffer = null;

      BufferHandle() {
      }

      ByteBuffer get(int size) {
         if(this.alignedBuffer != null && size <= this.alignedBuffer.capacity()) {
            return this.alignedBuffer;
         } else {
            if(this.alignedBuffer != null) {
               ChunkReader.bufferSize.getAndAdd((long)(-this.alignedBuffer.capacity()));
               FileUtils.clean(this.alignedBuffer, true);
               this.alignedBuffer = null;
            }

            this.alignedBuffer = BufferType.OFF_HEAP_ALIGNED.allocate(size);
            ChunkReader.bufferSize.getAndAdd((long)this.alignedBuffer.capacity());
            return this.alignedBuffer;
         }
      }

      void recycle() {
         if(ChunkReader.bufferSize.get() > ChunkReader.memoryLimit.longValue() || !ChunkReader.scratchBuffers.relaxedOffer(this)) {
            ChunkReader.bufferSize.getAndAdd((long)(-this.alignedBuffer.capacity()));
            FileUtils.clean(this.alignedBuffer, true);
         }

      }
   }
}
