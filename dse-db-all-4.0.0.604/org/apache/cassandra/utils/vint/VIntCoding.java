package org.apache.cassandra.utils.vint;

import io.netty.util.concurrent.FastThreadLocal;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class VIntCoding {
   public static final int MAX_SIZE = 10;
   protected static final FastThreadLocal<byte[]> encodingBuffer = new FastThreadLocal<byte[]>() {
      public byte[] initialValue() {
         return new byte[9];
      }
   };

   public VIntCoding() {
   }

   public static long readUnsignedVInt(DataInput input) throws IOException {
      int firstByte = input.readByte();
      if(firstByte >= 0) {
         return (long)firstByte;
      } else {
         int size = numberOfExtraBytesToRead(firstByte);
         long retval = (long)(firstByte & firstByteValueMask(size));

         for(int ii = 0; ii < size; ++ii) {
            byte b = input.readByte();
            retval <<= 8;
            retval |= (long)(b & 255);
         }

         return retval;
      }
   }

   public static long readVInt(DataInput input) throws IOException {
      return decodeZigZag64(readUnsignedVInt(input));
   }

   public static int firstByteValueMask(int extraBytesToRead) {
      return 255 >> extraBytesToRead;
   }

   public static int encodeExtraBytesToRead(int extraBytesToRead) {
      return ~firstByteValueMask(extraBytesToRead);
   }

   public static int numberOfExtraBytesToRead(int firstByte) {
      return Integer.numberOfLeadingZeros(~firstByte) - 24;
   }

   public static void writeUnsignedVInt(long value, DataOutput output) throws IOException {
      int size = computeUnsignedVIntSize(value);
      if(size == 1) {
         output.write((int)value);
      } else {
         output.write(encodeVInt(value, size), 0, size);
      }
   }

   public static byte[] encodeVInt(long value, int size) {
      byte[] encodingSpace = (byte[])encodingBuffer.get();
      int extraBytes = size - 1;

      for(int i = extraBytes; i >= 0; --i) {
         encodingSpace[i] = (byte)((int)value);
         value >>= 8;
      }

      encodingSpace[0] = (byte)(encodingSpace[0] | encodeExtraBytesToRead(extraBytes));
      return encodingSpace;
   }

   public static void writeVInt(long value, DataOutput output) throws IOException {
      writeUnsignedVInt(encodeZigZag64(value), output);
   }

   public static long decodeZigZag64(long n) {
      return n >>> 1 ^ -(n & 1L);
   }

   public static long encodeZigZag64(long n) {
      return n << 1 ^ n >> 63;
   }

   public static int computeVIntSize(long param) {
      return computeUnsignedVIntSize(encodeZigZag64(param));
   }

   public static int computeUnsignedVIntSize(long value) {
      int magnitude = Long.numberOfLeadingZeros(value | 1L);
      return 639 - magnitude * 9 >> 6;
   }
}
