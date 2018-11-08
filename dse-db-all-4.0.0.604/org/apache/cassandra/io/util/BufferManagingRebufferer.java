package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BiFunction;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.utils.memory.BufferPool;

public abstract class BufferManagingRebufferer implements Rebufferer {
   private static final BufferPool bufferPool = new BufferPool();
   private final Deque<BufferManagingRebufferer.BufferHolderImpl> buffers = new ArrayDeque();
   protected final ChunkReader source;

   abstract long alignedPosition(long var1);

   BufferManagingRebufferer(ChunkReader wrapped) {
      this.source = wrapped;
   }

   public void closeReader() {
   }

   public void close() {
      this.source.close();
   }

   public AsynchronousChannelProxy channel() {
      return this.source.channel();
   }

   public long fileLength() {
      return this.source.fileLength();
   }

   public Rebufferer.BufferHolder rebuffer(long position) {
      assert !TPC.isTPCThread();

      Rebufferer.BufferHolder ret = this.newBufferHolder(this.alignedPosition(position));

      try {
         this.source.readChunk(ret.offset(), ret.buffer()).join();
         return ret;
      } catch (CompletionException var5) {
         ret.release();
         if(var5.getCause() != null && var5.getCause() instanceof RuntimeException) {
            throw (RuntimeException)var5.getCause();
         } else {
            throw var5;
         }
      }
   }

   public CompletableFuture<Rebufferer.BufferHolder> rebufferAsync(long position) {
      Rebufferer.BufferHolder ret = this.newBufferHolder(this.alignedPosition(position));
      return this.source.readChunk(ret.offset(), ret.buffer()).handle((buffer, err) -> {
         if(err != null) {
            ret.release();
            throw new CompletionException(err);
         } else {
            return ret;
         }
      });
   }

   public int rebufferSize() {
      return this.source.chunkSize();
   }

   public Rebufferer.BufferHolder rebuffer(long position, Rebufferer.ReaderConstraint constraint) {
      if(constraint == Rebufferer.ReaderConstraint.ASYNC) {
         throw new UnsupportedOperationException("Async read is not supported without caching.");
      } else {
         return this.rebuffer(position);
      }
   }

   public double getCrcCheckChance() {
      return this.source.getCrcCheckChance();
   }

   public String toString() {
      return "BufferManagingRebufferer." + this.getClass().getSimpleName() + ':' + this.source;
   }

   Rebufferer.BufferHolder newBufferHolder(long offset) {
      BufferManagingRebufferer.BufferHolderImpl ret = (BufferManagingRebufferer.BufferHolderImpl)this.buffers.pollFirst();
      if(ret == null) {
         ret = new BufferManagingRebufferer.BufferHolderImpl();
      }

      return ret.init(offset);
   }

   static class Aligned extends BufferManagingRebufferer {
      Aligned(ChunkReader wrapped) {
         super(wrapped);

         assert Integer.bitCount(wrapped.chunkSize()) == 1;

      }

      long alignedPosition(long position) {
         return position & (long)(-this.source.chunkSize());
      }
   }

   static class Unaligned extends BufferManagingRebufferer {
      Unaligned(ChunkReader wrapped) {
         super(wrapped);
      }

      long alignedPosition(long position) {
         return position;
      }
   }

   private class BufferHolderImpl implements Rebufferer.BufferHolder {
      private long offset;
      private ByteBuffer buffer;

      private BufferHolderImpl() {
         this.offset = -1L;
         this.buffer = null;
      }

      private BufferManagingRebufferer.BufferHolderImpl init(long offset) {
         assert this.offset == -1L && this.buffer == null : "Attempted to initialize before releasing for previous use";

         this.offset = offset;
         this.buffer = BufferManagingRebufferer.bufferPool.get(BufferManagingRebufferer.this.source.chunkSize(), BufferManagingRebufferer.this.source.preferredBufferType()).order(ByteOrder.BIG_ENDIAN);
         this.buffer.limit(0);
         return this;
      }

      public ByteBuffer buffer() {
         return this.buffer;
      }

      public long offset() {
         return this.offset;
      }

      public void release() {
         assert this.offset != -1L && this.buffer != null : "released twice";

         BufferManagingRebufferer.bufferPool.put(this.buffer);
         this.buffer = null;
         this.offset = -1L;
         BufferManagingRebufferer.this.buffers.offerFirst(this);
      }
   }
}
