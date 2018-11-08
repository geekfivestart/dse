package org.apache.cassandra.utils;

import java.nio.ByteBuffer;

public class FastByteOperations {
   public FastByteOperations() {
   }

   public static int compareUnsigned(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      return FastByteOperations.BestHolder.BEST.compare(b1, s1, l1, b2, s2, l2);
   }

   public static int compareUnsigned(ByteBuffer b1, byte[] b2, int s2, int l2) {
      return FastByteOperations.BestHolder.BEST.compare(b1, b2, s2, l2);
   }

   public static int compareUnsigned(byte[] b1, int s1, int l1, ByteBuffer b2) {
      return -FastByteOperations.BestHolder.BEST.compare(b2, b1, s1, l1);
   }

   public static int compareUnsigned(ByteBuffer b1, ByteBuffer b2) {
      return FastByteOperations.BestHolder.BEST.compare(b1, b2);
   }

   public static void copy(ByteBuffer src, int srcPosition, byte[] trg, int trgPosition, int length) {
      FastByteOperations.BestHolder.BEST.copy(src, srcPosition, trg, trgPosition, length);
   }

   public static void copy(ByteBuffer src, int srcPosition, ByteBuffer trg, int trgPosition, int length) {
      FastByteOperations.BestHolder.BEST.copy(src, srcPosition, trg, trgPosition, length);
   }

   public static final class PureJavaOperations implements FastByteOperations.ByteOperations {
      public PureJavaOperations() {
      }

      public int compare(byte[] buffer1, int offset1, int length1, byte[] buffer2, int offset2, int length2) {
         if(buffer1 == buffer2 && offset1 == offset2 && length1 == length2) {
            return 0;
         } else {
            int end1 = offset1 + length1;
            int end2 = offset2 + length2;
            int i = offset1;

            for(int j = offset2; i < end1 && j < end2; ++j) {
               int a = buffer1[i] & 255;
               int b = buffer2[j] & 255;
               if(a != b) {
                  return a - b;
               }

               ++i;
            }

            return length1 - length2;
         }
      }

      public int compare(ByteBuffer buffer1, byte[] buffer2, int offset2, int length2) {
         return buffer1.hasArray()?this.compare(buffer1.array(), buffer1.arrayOffset() + buffer1.position(), buffer1.remaining(), buffer2, offset2, length2):this.compare(buffer1, ByteBuffer.wrap(buffer2, offset2, length2));
      }

      public int compare(ByteBuffer buffer1, ByteBuffer buffer2) {
         int end1 = buffer1.limit();
         int end2 = buffer2.limit();
         int i = buffer1.position();

         for(int j = buffer2.position(); i < end1 && j < end2; ++j) {
            int a = buffer1.get(i) & 255;
            int b = buffer2.get(j) & 255;
            if(a != b) {
               return a - b;
            }

            ++i;
         }

         return buffer1.remaining() - buffer2.remaining();
      }

      public void copy(ByteBuffer src, int srcPosition, byte[] trg, int trgPosition, int length) {
         if(src.hasArray()) {
            System.arraycopy(src.array(), src.arrayOffset() + srcPosition, trg, trgPosition, length);
         } else {
            src = src.duplicate();
            src.position(srcPosition);
            src.get(trg, trgPosition, length);
         }
      }

      public void copy(ByteBuffer src, int srcPosition, ByteBuffer trg, int trgPosition, int length) {
         if(src.hasArray() && trg.hasArray()) {
            System.arraycopy(src.array(), src.arrayOffset() + srcPosition, trg.array(), trg.arrayOffset() + trgPosition, length);
         } else {
            src = src.duplicate();
            src.position(srcPosition).limit(srcPosition + length);
            trg = trg.duplicate();
            trg.position(trgPosition);
            trg.put(src);
         }
      }
   }

   public static final class UnsafeOperations implements FastByteOperations.ByteOperations {
      public UnsafeOperations() {
      }

      public int compare(byte[] buffer1, int offset1, int length1, byte[] buffer2, int offset2, int length2) {
         return compare0(buffer1, UnsafeByteBufferAccess.BYTE_ARRAY_BASE_OFFSET + (long)offset1, length1, buffer2, UnsafeByteBufferAccess.BYTE_ARRAY_BASE_OFFSET + (long)offset2, length2);
      }

      public int compare(ByteBuffer buffer1, byte[] array2, int offset2, int length2) {
         return compare0(buffer1, array2, UnsafeByteBufferAccess.BYTE_ARRAY_BASE_OFFSET + (long)offset2, length2);
      }

      public int compare(ByteBuffer buffer1, ByteBuffer buffer2) {
         Object array2 = UnsafeByteBufferAccess.getArray(buffer2);
         int position2 = buffer2.position();
         long offset2 = UnsafeByteBufferAccess.bufferOffset(buffer2, array2) + (long)position2;
         int length2 = buffer2.limit() - position2;
         return compare0(buffer1, array2, offset2, length2);
      }

      public void copy(ByteBuffer srcBuf, int srcPosition, byte[] trg, int trgPosition, int length) {
         Object src = UnsafeByteBufferAccess.getArray(srcBuf);
         long srcOffset = UnsafeByteBufferAccess.bufferOffset(srcBuf, src) + (long)srcPosition;
         UnsafeCopy.copy0(src, srcOffset, trg, UnsafeByteBufferAccess.BYTE_ARRAY_BASE_OFFSET + (long)trgPosition, (long)length);
      }

      public void copy(ByteBuffer srcBuf, int srcPosition, ByteBuffer trgBuf, int trgPosition, int length) {
         if(trgBuf.isReadOnly()) {
            throw new IllegalArgumentException("Cannot copy into a read only ByteBuffer");
         } else {
            Object src = UnsafeByteBufferAccess.getArray(srcBuf);
            long srcOffset = UnsafeByteBufferAccess.bufferOffset(srcBuf, src) + (long)srcPosition;
            Object trg = UnsafeByteBufferAccess.getArray(trgBuf);
            long trgOffset = UnsafeByteBufferAccess.bufferOffset(trgBuf, trg) + (long)trgPosition;
            UnsafeCopy.copy0(src, srcOffset, trg, trgOffset, (long)length);
         }
      }

      public static int compare0(ByteBuffer buffer1, Object array2, long offset2, int length2) {
         Object array1 = UnsafeByteBufferAccess.getArray(buffer1);
         int position1 = buffer1.position();
         long offset1 = UnsafeByteBufferAccess.bufferOffset(buffer1, array1) + (long)position1;
         int length1 = buffer1.limit() - position1;
         return compare0(array1, offset1, length1, array2, offset2, length2);
      }

      public static int compare0(Object buffer1, long memoryOffset1, int length1, Object buffer2, long memoryOffset2, int length2) {
         int minLength = Math.min(length1, length2);
         int wordComparisons = minLength & -8;

         int i;
         for(i = 0; i < wordComparisons; i += 8) {
            long lw = UnsafeAccess.UNSAFE.getLong(buffer1, memoryOffset1 + (long)i);
            long rw = UnsafeAccess.UNSAFE.getLong(buffer2, memoryOffset2 + (long)i);
            if(lw != rw) {
               if(UnsafeMemoryAccess.BIG_ENDIAN) {
                  return Long.compareUnsigned(lw, rw);
               }

               return Long.compareUnsigned(Long.reverseBytes(lw), Long.reverseBytes(rw));
            }
         }

         for(i = wordComparisons; i < minLength; ++i) {
            int b1 = UnsafeAccess.UNSAFE.getByte(buffer1, memoryOffset1 + (long)i) & 255;
            int b2 = UnsafeAccess.UNSAFE.getByte(buffer2, memoryOffset2 + (long)i) & 255;
            if(b1 != b2) {
               return b1 - b2;
            }
         }

         return length1 - length2;
      }
   }

   private static class BestHolder {
      static final String UNSAFE_COMPARER_NAME = FastByteOperations.class.getName() + "$UnsafeOperations";
      static final FastByteOperations.ByteOperations BEST = getBest();

      private BestHolder() {
      }

      static FastByteOperations.ByteOperations getBest() {
         if(!Architecture.IS_UNALIGNED) {
            return new FastByteOperations.PureJavaOperations();
         } else {
            try {
               Class<?> theClass = Class.forName(UNSAFE_COMPARER_NAME);
               FastByteOperations.ByteOperations comparer = (FastByteOperations.ByteOperations)theClass.getConstructor(new Class[0]).newInstance(new Object[0]);
               return comparer;
            } catch (Throwable var2) {
               JVMStabilityInspector.inspectThrowable(var2);
               return new FastByteOperations.PureJavaOperations();
            }
         }
      }
   }

   public interface ByteOperations {
      int compare(byte[] var1, int var2, int var3, byte[] var4, int var5, int var6);

      int compare(ByteBuffer var1, byte[] var2, int var3, int var4);

      int compare(ByteBuffer var1, ByteBuffer var2);

      void copy(ByteBuffer var1, int var2, byte[] var3, int var4, int var5);

      void copy(ByteBuffer var1, int var2, ByteBuffer var3, int var4, int var5);
   }
}
