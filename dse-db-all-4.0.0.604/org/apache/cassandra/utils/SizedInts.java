package org.apache.cassandra.utils;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

public class SizedInts {
   public SizedInts() {
   }

   public static int nonZeroSize(long value) {
      if(value < 0L) {
         value = ~value;
      }

      int lz = Long.numberOfLeadingZeros(value);
      return (64 - lz + 1 + 7) / 8;
   }

   public static int sizeAllowingZero(long value) {
      return value == 0L?0:nonZeroSize(value);
   }

   public static long read(ByteBuffer src, int startPos, int bytes) {
      long high;
      switch(bytes) {
      case 0:
         return 0L;
      case 1:
         return (long)src.get(startPos);
      case 2:
         return (long)src.getShort(startPos);
      case 3:
         high = (long)src.get(startPos);
         return high << 16 | (long)src.getShort(startPos + 1) & 65535L;
      case 4:
         return (long)src.getInt(startPos);
      case 5:
         high = (long)src.get(startPos);
         return high << 32 | (long)src.getInt(startPos + 1) & 4294967295L;
      case 6:
         high = (long)src.getShort(startPos);
         return high << 32 | (long)src.getInt(startPos + 2) & 4294967295L;
      case 7:
         high = (long)src.get(startPos);
         high = high << 16 | (long)src.getShort(startPos + 1) & 65535L;
         return high << 32 | (long)src.getInt(startPos + 3) & 4294967295L;
      case 8:
         return src.getLong(startPos);
      default:
         throw new AssertionError();
      }
   }

   public static long readUnsigned(ByteBuffer src, int startPos, int bytes) {
      return read(src, startPos, bytes) & (1L << bytes * 8) - 1L;
   }

   public static void write(DataOutput dest, long value, int size) throws IOException {
      for(int i = size - 1; i >= 0; --i) {
         dest.writeByte((int)(value >> i * 8));
      }

   }
}
