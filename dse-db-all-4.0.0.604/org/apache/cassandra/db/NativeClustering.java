package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.UnsafeByteBufferAccess;
import org.apache.cassandra.utils.UnsafeCopy;
import org.apache.cassandra.utils.UnsafeMemoryAccess;
import org.apache.cassandra.utils.memory.NativeAllocator;

public class NativeClustering extends AbstractClusteringPrefix implements Clustering {
   private static final long EMPTY_SIZE = ObjectSizes.measure(new NativeClustering());
   private final long peer;

   private NativeClustering() {
      this.peer = 0L;
   }

   public NativeClustering(NativeAllocator allocator, Clustering clustering) {
      int count = clustering.size();
      int metadataSize = count * 2 + 4;
      int dataSize = clustering.dataSize();
      int bitmapSize = count + 7 >>> 3;

      assert count < 65536;

      assert dataSize < 65536;

      this.peer = allocator.allocate(metadataSize + dataSize + bitmapSize);
      long bitmapStart = this.peer + (long)metadataSize;
      UnsafeMemoryAccess.setShort(this.peer, (short)count);
      UnsafeMemoryAccess.setShort(this.peer + (long)(metadataSize - 2), (short)dataSize);
      UnsafeMemoryAccess.fill(bitmapStart, (long)bitmapSize,(byte) 0);
      long dataStart = this.peer + (long)metadataSize + (long)bitmapSize;
      int dataOffset = 0;

      for(int i = 0; i < count; ++i) {
         UnsafeMemoryAccess.setShort(this.peer + 2L + (long)(i * 2), (short)dataOffset);
         ByteBuffer value = clustering.get(i);
         if(value == null) {
            long boffset = bitmapStart + (long)(i >>> 3);
            int b = UnsafeMemoryAccess.getByte(boffset);
            b = b | 1 << (i & 7);
            UnsafeMemoryAccess.setByte(boffset, (byte)b);
         } else {
            assert value.order() == ByteOrder.BIG_ENDIAN;

            int size = value.remaining();
            UnsafeCopy.copyBufferToMemory(dataStart + (long)dataOffset, value);
            dataOffset += size;
         }
      }

   }

   public ClusteringPrefix.Kind kind() {
      return ClusteringPrefix.Kind.CLUSTERING;
   }

   public int size() {
      return UnsafeMemoryAccess.getUnsignedShort(this.peer);
   }

   public ByteBuffer get(int i) {
      int size = this.size();
      if(i >= size) {
         throw new IndexOutOfBoundsException();
      } else {
         int metadataSize = size * 2 + 4;
         int bitmapSize = size + 7 >>> 3;
         long bitmapStart = this.peer + (long)metadataSize;
         int b = UnsafeMemoryAccess.getByte(bitmapStart + (long)(i >>> 3));
         if((b & 1 << (i & 7)) != 0) {
            return null;
         } else {
            int startOffset = UnsafeMemoryAccess.getUnsignedShort(this.peer + 2L + (long)(i * 2));
            int endOffset = UnsafeMemoryAccess.getUnsignedShort(this.peer + 4L + (long)(i * 2));
            long bufferAddress = bitmapStart + (long)bitmapSize + (long)startOffset;
            int bufferLength = endOffset - startOffset;
            return UnsafeByteBufferAccess.getByteBuffer(bufferAddress, bufferLength, ByteOrder.BIG_ENDIAN);
         }
      }
   }

   public ByteBuffer[] getRawValues() {
      ByteBuffer[] values = new ByteBuffer[this.size()];

      for(int i = 0; i < values.length; ++i) {
         values[i] = this.get(i);
      }

      return values;
   }

   public long unsharedHeapSize() {
      return EMPTY_SIZE;
   }

   public long unsharedHeapSizeExcludingData() {
      return EMPTY_SIZE;
   }
}
