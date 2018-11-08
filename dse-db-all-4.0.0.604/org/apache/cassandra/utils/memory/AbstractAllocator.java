package org.apache.cassandra.utils.memory;

import java.nio.ByteBuffer;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.rows.ArrayBackedRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class AbstractAllocator {
   public AbstractAllocator() {
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

   public abstract ByteBuffer allocate(int var1);

   public Row.Builder cloningRowBuilder(int size) {
      return new AbstractAllocator.CloningRowBuilder(this, size);
   }

   public Row.Builder cloningRowBuilder() {
      return new AbstractAllocator.CloningRowBuilder(this);
   }

   public static class CloningRowBuilder extends ArrayBackedRow.Builder {
      private final AbstractAllocator allocator;

      private CloningRowBuilder(AbstractAllocator allocator, int size) {
         super(true, -2147483648, size);
         this.allocator = allocator;
      }

      private CloningRowBuilder(AbstractAllocator allocator) {
         super(true, -2147483648);
         this.allocator = allocator;
      }

      public void newRow(Clustering clustering) {
         super.newRow(clustering.copy(this.allocator));
      }

      public void addCell(Cell cell) {
         super.addCell(cell.copy(this.allocator));
      }
   }
}
