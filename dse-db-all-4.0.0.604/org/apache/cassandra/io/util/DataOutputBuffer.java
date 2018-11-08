package org.apache.cassandra.io.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.netty.util.concurrent.FastThreadLocal;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class DataOutputBuffer extends BufferedDataOutputStreamPlus {
   static final long DOUBLING_THRESHOLD = Long.getLong("cassandra.DOB_DOUBLING_THRESHOLD_MB", 64L).longValue();
   private static final int MAX_RECYCLE_BUFFER_SIZE = Integer.getInteger("cassandra.dob_max_recycle_bytes", 1048576).intValue();
   private static final int DEFAULT_INITIAL_BUFFER_SIZE = 128;
   public static final FastThreadLocal<DataOutputBuffer> scratchBuffer = new FastThreadLocal<DataOutputBuffer>() {
      protected DataOutputBuffer initialValue() throws Exception {
         return new DataOutputBuffer() {
            public void close() {
               if(this.buffer.capacity() <= DataOutputBuffer.MAX_RECYCLE_BUFFER_SIZE) {
                  this.buffer.clear();
               } else {
                  this.buffer = ByteBuffer.allocate(128);
               }

            }
         };
      }
   };
   @VisibleForTesting
   static final int MAX_ARRAY_SIZE = 2147483639;

   public DataOutputBuffer() {
      this(128);
   }

   public DataOutputBuffer(int size) {
      this(ByteBuffer.allocate(size));
   }

   protected DataOutputBuffer(ByteBuffer buffer) {
      super(buffer);
   }

   public void flush() throws IOException {
      throw new UnsupportedOperationException();
   }

   @VisibleForTesting
   static int saturatedArraySizeCast(long size) {
      Preconditions.checkArgument(size >= 0L);
      return (int)Math.min(2147483639L, size);
   }

   @VisibleForTesting
   static int checkedArraySizeCast(long size) {
      Preconditions.checkArgument(size >= 0L);
      Preconditions.checkArgument(size <= 2147483639L);
      return (int)size;
   }

   protected void doFlush(int count) throws IOException {
      this.expandToFit((long)count);
   }

   @VisibleForTesting
   long capacity() {
      return (long)this.buffer.capacity();
   }

   @VisibleForTesting
   long validateReallocation(long newSize) {
      int saturatedSize = saturatedArraySizeCast(newSize);
      if((long)saturatedSize <= this.capacity()) {
         throw new RuntimeException();
      } else {
         return (long)saturatedSize;
      }
   }

   @VisibleForTesting
   long calculateNewSize(long count) {
      long capacity = this.capacity();
      long newSize = capacity + count;
      if(capacity > 1048576L * DOUBLING_THRESHOLD) {
         newSize = Math.max(capacity * 3L / 2L, newSize);
      } else {
         newSize = Math.max(capacity * 2L, newSize);
      }

      return this.validateReallocation(newSize);
   }

   protected void expandToFit(long count) {
      if(count > 0L) {
         ByteBuffer newBuffer = ByteBuffer.allocate(checkedArraySizeCast(this.calculateNewSize(count)));
         this.buffer.flip();
         newBuffer.put(this.buffer);
         this.buffer = newBuffer;
      }
   }

   protected WritableByteChannel newDefaultChannel() {
      return new DataOutputBuffer.GrowingChannel();
   }

   public void clear() {
      this.buffer.clear();
   }

   public void close() {
   }

   public ByteBuffer buffer() {
      ByteBuffer result = this.buffer.duplicate();
      result.flip();
      return result;
   }

   public byte[] getData() {
      assert this.buffer.arrayOffset() == 0;

      return this.buffer.array();
   }

   public int getLength() {
      return this.buffer.position();
   }

   public boolean hasPosition() {
      return true;
   }

   public long position() {
      return (long)this.getLength();
   }

   public ByteBuffer asNewBuffer() {
      return ByteBuffer.wrap(this.toByteArray());
   }

   public ByteBuffer trimmedBuffer() {
      return ByteBuffer.wrap(this.toByteArray());
   }

   public byte[] toByteArray() {
      ByteBuffer buffer = this.buffer();
      byte[] result = new byte[buffer.remaining()];
      buffer.get(result);
      return result;
   }

   @VisibleForTesting
   final class GrowingChannel implements WritableByteChannel {
      GrowingChannel() {
      }

      public int write(ByteBuffer src) {
         int count = src.remaining();
         DataOutputBuffer.this.expandToFit((long)count);
         DataOutputBuffer.this.buffer.put(src);
         return count;
      }

      public boolean isOpen() {
         return true;
      }

      public void close() {
      }
   }
}
