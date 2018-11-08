package org.apache.cassandra.utils.memory;

import java.nio.ByteBuffer;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.Row;

public abstract class MemtableBufferAllocator extends MemtableAllocator {
   private final ContextAllocator allocator = new ContextAllocator(this);

   protected MemtableBufferAllocator(MemtableAllocator.SubAllocator onHeap, MemtableAllocator.SubAllocator offHeap) {
      super(onHeap, offHeap);
   }

   public Row.Builder rowBuilder() {
      return this.allocator.cloningRowBuilder();
   }

   public DecoratedKey clone(DecoratedKey key) {
      return new BufferDecoratedKey(key.getToken(), this.allocator.clone(key.getKey()));
   }

   public abstract ByteBuffer allocate(int var1);
}
