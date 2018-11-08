package org.apache.cassandra.utils.memory;

import java.nio.ByteBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;

public final class ContextAllocator extends AbstractAllocator {
   private final MemtableBufferAllocator allocator;

   public ContextAllocator(MemtableBufferAllocator allocator) {
      this.allocator = allocator;
   }

   public ByteBuffer clone(ByteBuffer buffer) {
      assert buffer != null;

      if(buffer.remaining() == 0) {
         return ByteBufferUtil.EMPTY_BYTE_BUFFER;
      } else {
         ByteBuffer cloned = this.allocate(buffer.remaining());
         cloned.mark();
         cloned.put(buffer.duplicate());
         cloned.reset();
         return cloned;
      }
   }

   public ByteBuffer allocate(int size) {
      return this.allocator.allocate(size);
   }
}
