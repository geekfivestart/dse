package org.apache.cassandra.utils;

import java.nio.ByteBuffer;

public abstract class UnsafeCopy {
   private static final long UNSAFE_COPY_THRESHOLD = 1048576L;
   private static final long MIN_COPY_THRESHOLD = 6L;

   public UnsafeCopy() {
   }

   public static void copyBufferToMemory(long tgtAddress, ByteBuffer srcBuffer) {
      copyBufferToMemory(srcBuffer, srcBuffer.position(), tgtAddress, srcBuffer.remaining());
   }

   public static void copyMemoryToArray(long srcAddress, byte[] trg, int trgPosition, int length) {
      copy0((Object)null, srcAddress, trg, UnsafeByteBufferAccess.BYTE_ARRAY_BASE_OFFSET + (long)trgPosition, (long)length);
   }

   public static void copyBufferToMemory(ByteBuffer srcBuf, int srcPosition, long addressDst, int length) {
      Object src = UnsafeByteBufferAccess.getArray(srcBuf);
      long srcOffset = UnsafeByteBufferAccess.bufferOffset(srcBuf, src);
      copy0(src, srcOffset + (long)srcPosition, (Object)null, addressDst, (long)length);
   }

   public static void copyArrayToMemory(byte[] src, int srcPosition, long addressDst, int length) {
      copy0(src, UnsafeByteBufferAccess.BYTE_ARRAY_BASE_OFFSET + (long)srcPosition, (Object)null, addressDst, (long)length);
   }

   public static void copyMemoryToMemory(long addressSrc, long addressDst, long length) {
      copy0((Object)null, addressSrc, (Object)null, addressDst, length);
   }

   public static void copy0(Object src, long srcOffset, Object dst, long dstOffset, long length) {
      if(length > 6L) {
         while(length > 0L) {
            long size = length > 1048576L?1048576L:length;
            UnsafeAccess.UNSAFE.copyMemory(src, srcOffset, dst, dstOffset, size);
            length -= size;
            srcOffset += size;
            dstOffset += size;
         }

      } else {
         for(int i = 0; (long)i < length; ++i) {
            byte b = UnsafeAccess.UNSAFE.getByte(src, srcOffset + (long)i);
            UnsafeAccess.UNSAFE.putByte(dst, dstOffset + (long)i, b);
         }

      }
   }
}
