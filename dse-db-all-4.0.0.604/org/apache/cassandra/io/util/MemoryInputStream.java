package org.apache.cassandra.io.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;
import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.cassandra.utils.UnsafeByteBufferAccess;

public class MemoryInputStream extends RebufferingInputStream implements DataInput {
   private final Memory mem;
   private final int bufferSize;
   private long offset;

   public MemoryInputStream(Memory mem) {
      this(mem, Ints.saturatedCast(mem.size));
   }

   @VisibleForTesting
   public MemoryInputStream(Memory mem, int bufferSize) {
      super(getByteBuffer(mem.peer, bufferSize));
      this.mem = mem;
      this.bufferSize = bufferSize;
      this.offset = mem.peer + (long)bufferSize;
   }

   protected void reBuffer() throws IOException {
      if(this.offset - this.mem.peer < this.mem.size()) {
         this.buffer = getByteBuffer(this.offset, Math.min(this.bufferSize, Ints.saturatedCast(this.memRemaining())));
         this.offset += (long)this.buffer.capacity();
      }
   }

   public int available() {
      return Ints.saturatedCast((long)this.buffer.remaining() + this.memRemaining());
   }

   private long memRemaining() {
      return this.mem.size + this.mem.peer - this.offset;
   }

   private static ByteBuffer getByteBuffer(long offset, int length) {
      return UnsafeByteBufferAccess.getByteBuffer(offset, length, ByteOrder.BIG_ENDIAN);
   }
}
