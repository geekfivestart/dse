package org.apache.cassandra.io.compress;

import java.nio.ByteBuffer;
import org.apache.cassandra.utils.UnsafeByteBufferAccess;
import org.apache.cassandra.utils.UnsafeMemoryAccess;
import sun.misc.VM;

public enum BufferType {
   ON_HEAP {
      public ByteBuffer allocate(int size) {
         return ByteBuffer.allocate(size);
      }
   },
   OFF_HEAP {
      public ByteBuffer allocate(int size) {
         return ByteBuffer.allocateDirect(size);
      }
   },
   OFF_HEAP_ALIGNED {
      public ByteBuffer allocate(int size) {
         return BufferType.allocateDirectAligned(size);
      }
   };

   private BufferType() {
   }

   public abstract ByteBuffer allocate(int var1);

   public static BufferType typeOf(ByteBuffer buffer) {
      return buffer.isDirect()?((UnsafeByteBufferAccess.getAddress(buffer) & (long)(-UnsafeMemoryAccess.pageSize())) == 0L?OFF_HEAP_ALIGNED:OFF_HEAP):ON_HEAP;
   }

   private static ByteBuffer allocateDirectAligned(int capacity) {
      if(VM.isDirectMemoryPageAligned()) {
         return ByteBuffer.allocateDirect(capacity);
      } else {
         int align = UnsafeMemoryAccess.pageSize();
         if(Integer.bitCount(align) != 1) {
            throw new IllegalArgumentException("Alignment must be a power of 2");
         } else {
            ByteBuffer buffer = ByteBuffer.allocateDirect(capacity + align);
            long address = UnsafeByteBufferAccess.getAddress(buffer);
            long offset = address & (long)(align - 1);
            if(offset == 0L) {
               buffer.limit(capacity);
            } else {
               int pos = (int)((long)align - offset);
               buffer.position(pos);
               buffer.limit(pos + capacity);
            }

            return buffer.slice();
         }
      }
   }
}
