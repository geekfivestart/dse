package org.apache.cassandra.db.rows;

import com.google.common.hash.Hasher;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.HashingUtils;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.memory.AbstractAllocator;

public abstract class CellPath {
   public static final CellPath BOTTOM = new CellPath.EmptyCellPath();
   public static final CellPath TOP = new CellPath.EmptyCellPath();

   public CellPath() {
   }

   public abstract int size();

   public abstract ByteBuffer get(int var1);

   public static CellPath create(ByteBuffer value) {
      assert value != null;

      return new CellPath.SingleItemCellPath(value);
   }

   public int dataSize() {
      int size = 0;

      for(int i = 0; i < this.size(); ++i) {
         size += this.get(i).remaining();
      }

      return size;
   }

   public void digest(Hasher hasher) {
      for(int i = 0; i < this.size(); ++i) {
         HashingUtils.updateBytes(hasher, this.get(i).duplicate());
      }

   }

   public abstract CellPath copy(AbstractAllocator var1);

   public abstract long unsharedHeapSizeExcludingData();

   public final int hashCode() {
      int result = 31;

      for(int i = 0; i < this.size(); ++i) {
         result += 31 * Objects.hash(new Object[]{this.get(i)});
      }

      return result;
   }

   public final boolean equals(Object o) {
      if(!(o instanceof CellPath)) {
         return false;
      } else {
         CellPath that = (CellPath)o;
         if(this.size() != that.size()) {
            return false;
         } else {
            for(int i = 0; i < this.size(); ++i) {
               if(!Objects.equals(this.get(i), that.get(i))) {
                  return false;
               }
            }

            return true;
         }
      }
   }

   private static class EmptyCellPath extends CellPath {
      private EmptyCellPath() {
      }

      public int size() {
         return 0;
      }

      public ByteBuffer get(int i) {
         throw new UnsupportedOperationException();
      }

      public CellPath copy(AbstractAllocator allocator) {
         return this;
      }

      public long unsharedHeapSizeExcludingData() {
         return 0L;
      }
   }

   private static class SingleItemCellPath extends CellPath {
      private static final long EMPTY_SIZE;
      protected final ByteBuffer value;

      private SingleItemCellPath(ByteBuffer value) {
         this.value = value;
      }

      public int size() {
         return 1;
      }

      public ByteBuffer get(int i) {
         assert i == 0;

         return this.value;
      }

      public CellPath copy(AbstractAllocator allocator) {
         return new CellPath.SingleItemCellPath(allocator.clone(this.value));
      }

      public long unsharedHeapSizeExcludingData() {
         return EMPTY_SIZE + ObjectSizes.sizeOnHeapExcludingData(this.value);
      }

      static {
         EMPTY_SIZE = ObjectSizes.measure(new CellPath.SingleItemCellPath(ByteBufferUtil.EMPTY_BYTE_BUFFER));
      }
   }

   public interface Serializer {
      void serialize(CellPath var1, DataOutputPlus var2) throws IOException;

      CellPath deserialize(DataInputPlus var1) throws IOException;

      long serializedSize(CellPath var1);

      void skip(DataInputPlus var1) throws IOException;
   }
}
