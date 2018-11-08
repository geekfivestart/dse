package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UnsafeByteBufferAccess;
import org.apache.cassandra.utils.UnsafeCopy;
import org.apache.cassandra.utils.UnsafeMemoryAccess;
import org.apache.cassandra.utils.concurrent.Ref;

public class Memory implements AutoCloseable {
   protected long peer;
   protected final long size;

   protected Memory(long bytes) {
      if(bytes <= 0L) {
         throw new AssertionError();
      } else {
         this.size = bytes;
         this.peer = UnsafeMemoryAccess.allocate(this.size);
         if(this.peer == 0L) {
            throw new OutOfMemoryError();
         }
      }
   }

   protected Memory(Memory copyOf) {
      this.size = copyOf.size;
      this.peer = copyOf.peer;
   }

   public static Memory allocate(long bytes) {
      if(bytes < 0L) {
         throw new IllegalArgumentException();
      } else {
         return (Memory)(Ref.DEBUG_ENABLED?new SafeMemory(bytes):new Memory(bytes));
      }
   }

   public void setByte(long offset, byte b) {
      this.checkBounds(offset, offset + 1L);
      UnsafeMemoryAccess.setByte(this.peer + offset, b);
   }

   public void setMemory(long offset, long bytes, byte b) {
      this.checkBounds(offset, offset + bytes);
      UnsafeMemoryAccess.fill(this.peer + offset, bytes, b);
   }

   public void setLong(long offset, long l) {
      this.checkBounds(offset, offset + 8L);
      UnsafeMemoryAccess.setLong(this.peer + offset, l);
   }

   public void setInt(long offset, int i) {
      this.checkBounds(offset, offset + 4L);
      UnsafeMemoryAccess.setInt(this.peer + offset, i);
   }

   public void setBytes(long memoryOffset, ByteBuffer buffer) {
      if(buffer == null) {
         throw new NullPointerException();
      } else if(buffer.remaining() != 0) {
         this.checkBounds(memoryOffset, memoryOffset + (long)buffer.remaining());
         UnsafeCopy.copyBufferToMemory(buffer, buffer.position(), this.peer + memoryOffset, buffer.remaining());
      }
   }

   public void setBytes(long memoryOffset, byte[] buffer, int bufferOffset, int count) {
      if(buffer == null) {
         throw new NullPointerException();
      } else if(bufferOffset >= 0 && count >= 0 && bufferOffset + count <= buffer.length) {
         if(count != 0) {
            this.checkBounds(memoryOffset, memoryOffset + (long)count);
            UnsafeCopy.copyArrayToMemory(buffer, bufferOffset, this.peer + memoryOffset, count);
         }
      } else {
         throw new IndexOutOfBoundsException();
      }
   }

   public byte getByte(long offset) {
      this.checkBounds(offset, offset + 1L);
      return UnsafeMemoryAccess.getByte(this.peer + offset);
   }

   public long getLong(long offset) {
      this.checkBounds(offset, offset + 8L);
      return UnsafeMemoryAccess.getLong(this.peer + offset);
   }

   public int getInt(long offset) {
      this.checkBounds(offset, offset + 4L);
      return UnsafeMemoryAccess.getInt(this.peer + offset);
   }

   public void getBytes(long memoryOffset, byte[] buffer, int bufferOffset, int count) {
      if(buffer == null) {
         throw new NullPointerException();
      } else if(bufferOffset >= 0 && count >= 0 && count <= buffer.length - bufferOffset) {
         if(count != 0) {
            this.checkBounds(memoryOffset, memoryOffset + (long)count);
            UnsafeCopy.copyMemoryToArray(this.peer + memoryOffset, buffer, bufferOffset, count);
         }
      } else {
         throw new IndexOutOfBoundsException();
      }
   }

   protected void checkBounds(long start, long end) {
      assert this.peer != 0L : "Memory was freed";

      assert start >= 0L && end <= this.size && start <= end : "Illegal bounds [" + start + ".." + end + "); size: " + this.size;
   }

   public void put(long trgOffset, Memory memory, long srcOffset, long size) {
      this.checkBounds(trgOffset, trgOffset + size);
      memory.checkBounds(srcOffset, srcOffset + size);
      UnsafeCopy.copyMemoryToMemory(memory.peer + srcOffset, this.peer + trgOffset, size);
   }

   public Memory copy(long newSize) {
      Memory copy = allocate(newSize);
      copy.put(0L, this, 0L, Math.min(this.size(), newSize));
      return copy;
   }

   public void free() {
      if(this.peer != 0L) {
         UnsafeMemoryAccess.free(this.peer);
      } else {
         assert this.size == 0L;
      }

      this.peer = 0L;
   }

   public void close() {
      this.free();
   }

   public long size() {
      assert this.peer != 0L;

      return this.size;
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof Memory)) {
         return false;
      } else {
         Memory b = (Memory)o;
         return this.peer == b.peer && this.size == b.size;
      }
   }

   public ByteBuffer[] asByteBuffers(long offset, long length) {
      this.checkBounds(offset, offset + length);
      if(this.size() == 0L) {
         return ByteBufferUtil.EMPTY_BUFFER_ARRAY;
      } else {
         ByteBuffer[] result = new ByteBuffer[(int)(length / 2147483647L) + 1];
         int size = (int)(this.size() / (long)result.length);

         for(int i = 0; i < result.length - 1; ++i) {
            result[i] = UnsafeByteBufferAccess.getByteBuffer(this.peer + offset, size);
            offset += (long)size;
            length -= (long)size;
         }

         result[result.length - 1] = UnsafeByteBufferAccess.getByteBuffer(this.peer + offset, (int)length);
         return result;
      }
   }

   public ByteBuffer asByteBuffer(long offset, int length) {
      this.checkBounds(offset, offset + (long)length);
      return UnsafeByteBufferAccess.getByteBuffer(this.peer + offset, length);
   }

   public void setByteBuffer(ByteBuffer buffer, long offset, int length) {
      this.checkBounds(offset, offset + (long)length);
      UnsafeByteBufferAccess.setByteBuffer(buffer, this.peer + offset, length);
   }

   public String toString() {
      return toString(this.peer, this.size);
   }

   protected static String toString(long peer, long size) {
      return String.format("Memory@[%x..%x)", new Object[]{Long.valueOf(peer), Long.valueOf(peer + size)});
   }
}
