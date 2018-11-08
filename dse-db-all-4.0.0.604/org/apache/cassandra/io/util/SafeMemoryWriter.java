package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class SafeMemoryWriter extends DataOutputBuffer {
   private SafeMemory memory;

   public SafeMemoryWriter(long initialCapacity) {
      this(new SafeMemory(initialCapacity));
   }

   private SafeMemoryWriter(SafeMemory memory) {
      super(tailBuffer(memory).order(ByteOrder.BIG_ENDIAN));
      this.memory = memory;
   }

   public SafeMemory currentBuffer() {
      return this.memory;
   }

   protected void expandToFit(long count) {
      this.resizeTo(this.calculateNewSize(count));
   }

   private void resizeTo(long newCapacity) {
      if(newCapacity != this.capacity()) {
         long position = this.length();
         ByteOrder order = this.buffer.order();
         SafeMemory oldBuffer = this.memory;
         this.memory = this.memory.copy(newCapacity);
         this.buffer = tailBuffer(this.memory);
         int newPosition = (int)(position - tailOffset(this.memory));
         this.buffer.position(newPosition);
         this.buffer.order(order);
         oldBuffer.free();
      }

   }

   public void trim() {
      this.resizeTo(this.length());
   }

   public void close() {
      this.memory.close();
   }

   public Throwable close(Throwable accumulate) {
      return this.memory.close(accumulate);
   }

   public long length() {
      return tailOffset(this.memory) + (long)this.buffer.position();
   }

   public long capacity() {
      return this.memory.size();
   }

   public SafeMemoryWriter order(ByteOrder order) {
      super.order(order);
      return this;
   }

   public long validateReallocation(long newSize) {
      return Math.min(newSize, this.length() + 2147483647L);
   }

   private static long tailOffset(Memory memory) {
      return Math.max(0L, memory.size - 2147483647L);
   }

   private static ByteBuffer tailBuffer(Memory memory) {
      return memory.asByteBuffer(tailOffset(memory), (int)Math.min(memory.size, 2147483647L));
   }
}
