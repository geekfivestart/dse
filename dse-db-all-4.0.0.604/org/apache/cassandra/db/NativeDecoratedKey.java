package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.UnsafeByteBufferAccess;
import org.apache.cassandra.utils.UnsafeCopy;
import org.apache.cassandra.utils.UnsafeMemoryAccess;
import org.apache.cassandra.utils.memory.NativeAllocator;

public class NativeDecoratedKey extends DecoratedKey {
   final long peer;

   public NativeDecoratedKey(Token token, NativeAllocator allocator, ByteBuffer key) {
      super(token);

      assert key != null;

      assert key.order() == ByteOrder.BIG_ENDIAN;

      int size = key.remaining();
      this.peer = allocator.allocate(4 + size);
      UnsafeMemoryAccess.setInt(this.peer, size);
      UnsafeCopy.copyBufferToMemory(this.peer + 4L, key);
   }

   public ByteBuffer getKey() {
      return UnsafeByteBufferAccess.getByteBuffer(this.peer + 4L, UnsafeMemoryAccess.getInt(this.peer), ByteOrder.BIG_ENDIAN);
   }
}
