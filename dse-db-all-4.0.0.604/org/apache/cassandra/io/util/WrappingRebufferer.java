package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Function;
import javax.annotation.Nullable;

public class WrappingRebufferer implements Rebufferer {
   protected final Rebufferer source;
   private final Deque<WrappingRebufferer.WrappingBufferHolder> buffers;

   public WrappingRebufferer(Rebufferer source) {
      this.source = source;
      this.buffers = new ConcurrentLinkedDeque();
   }

   public Rebufferer.BufferHolder rebuffer(long position, Rebufferer.ReaderConstraint constraint) {
      Rebufferer.BufferHolder bufferHolder = this.source.rebuffer(position, constraint);
      return this.newBufferHolder().initialize(bufferHolder, bufferHolder.buffer(), bufferHolder.offset());
   }

   public CompletableFuture<Rebufferer.BufferHolder> rebufferAsync(long position) {
      return this.source.rebufferAsync(position).thenApply((bufferHolder) -> {
         return this.newBufferHolder().initialize(bufferHolder, bufferHolder.buffer(), bufferHolder.offset());
      });
   }

   public int rebufferSize() {
      return this.source.rebufferSize();
   }

   protected WrappingRebufferer.WrappingBufferHolder newBufferHolder() {
      WrappingRebufferer.WrappingBufferHolder ret = (WrappingRebufferer.WrappingBufferHolder)this.buffers.pollFirst();
      if(ret == null) {
         ret = new WrappingRebufferer.WrappingBufferHolder();
      }

      return ret;
   }

   public AsynchronousChannelProxy channel() {
      return this.source.channel();
   }

   public long fileLength() {
      return this.source.fileLength();
   }

   public double getCrcCheckChance() {
      return this.source.getCrcCheckChance();
   }

   public void close() {
      this.source.close();
   }

   public void closeReader() {
      this.source.closeReader();
   }

   protected String paramsToString() {
      return "";
   }

   public String toString() {
      return this.getClass().getSimpleName() + '[' + this.paramsToString() + "]:" + this.source.toString();
   }

   protected final class WrappingBufferHolder implements Rebufferer.BufferHolder {
      @Nullable
      private Rebufferer.BufferHolder bufferHolder;
      private ByteBuffer buffer;
      private long offset;

      protected WrappingBufferHolder() {
      }

      protected WrappingRebufferer.WrappingBufferHolder initialize(@Nullable Rebufferer.BufferHolder bufferHolder, ByteBuffer buffer, long offset) {
         assert this.bufferHolder == null && this.buffer == null && this.offset == 0L : "initialized before release";

         this.bufferHolder = bufferHolder;
         this.buffer = buffer;
         this.offset = offset;
         return this;
      }

      public ByteBuffer buffer() {
         return this.buffer;
      }

      public long offset() {
         return this.offset;
      }

      public void offset(long offet) {
         this.offset = offet;
      }

      public int limit() {
         return this.buffer.limit();
      }

      public void limit(int limit) {
         this.buffer.limit(limit);
      }

      public void release() {
         assert this.buffer != null : "released twice";

         if(this.bufferHolder != null) {
            this.bufferHolder.release();
            this.bufferHolder = null;
         }

         this.buffer = null;
         this.offset = 0L;
         WrappingRebufferer.this.buffers.offerFirst(this);
      }
   }
}
