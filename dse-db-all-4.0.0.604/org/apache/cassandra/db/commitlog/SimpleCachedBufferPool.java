package org.apache.cassandra.db.commitlog;

import io.netty.util.concurrent.FastThreadLocal;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.FileUtils;

public class SimpleCachedBufferPool {
   protected static final FastThreadLocal<ByteBuffer> reusableBufferHolder = new FastThreadLocal<ByteBuffer>() {
      protected ByteBuffer initialValue() {
         return ByteBuffer.allocate(0);
      }
   };
   private Queue<ByteBuffer> bufferPool = new ConcurrentLinkedQueue();
   private AtomicInteger usedBuffers = new AtomicInteger(0);
   private final int maxBufferPoolSize;
   private final int bufferSize;
   private BufferType preferredReusableBufferType;

   public SimpleCachedBufferPool(int maxBufferPoolSize, int bufferSize) {
      this.preferredReusableBufferType = BufferType.ON_HEAP;
      this.maxBufferPoolSize = maxBufferPoolSize;
      this.bufferSize = bufferSize;
   }

   public ByteBuffer createBuffer(BufferType bufferType) {
      this.usedBuffers.incrementAndGet();
      ByteBuffer buf = (ByteBuffer)this.bufferPool.poll();
      if(buf != null) {
         buf.clear();
         return buf;
      } else {
         return bufferType.allocate(this.bufferSize);
      }
   }

   public ByteBuffer getThreadLocalReusableBuffer(int size) {
      ByteBuffer result = (ByteBuffer)reusableBufferHolder.get();
      if(result.capacity() < size || BufferType.typeOf(result) != this.preferredReusableBufferType) {
         FileUtils.clean(result);
         result = this.preferredReusableBufferType.allocate(size);
         reusableBufferHolder.set(result);
      }

      return result;
   }

   public void setPreferredReusableBufferType(BufferType type) {
      this.preferredReusableBufferType = type;
   }

   public void releaseBuffer(ByteBuffer buffer) {
      this.usedBuffers.decrementAndGet();
      if(this.bufferPool.size() < this.maxBufferPoolSize) {
         this.bufferPool.add(buffer);
      } else {
         FileUtils.clean(buffer);
      }

   }

   public void shutdown() {
      this.bufferPool.clear();
   }

   public boolean atLimit() {
      return this.usedBuffers.get() >= this.maxBufferPoolSize;
   }

   public String toString() {
      return "SimpleBufferPool:" + " bufferCount:" + this.usedBuffers.get() + ", bufferSize:" + this.maxBufferPoolSize + ", buffer size:" + this.bufferSize;
   }
}
