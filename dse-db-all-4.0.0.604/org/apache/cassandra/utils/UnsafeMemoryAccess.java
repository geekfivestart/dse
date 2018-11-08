package org.apache.cassandra.utils;

import com.sun.jna.Native;
import java.nio.ByteOrder;

public class UnsafeMemoryAccess {
   public static final boolean BIG_ENDIAN;

   public UnsafeMemoryAccess() {
   }

   public static void setByte(long address, byte b) {
      UnsafeAccess.UNSAFE.putByte(address, b);
   }

   public static void setShort(long address, short s) {
      if(Architecture.IS_UNALIGNED) {
         UnsafeAccess.UNSAFE.putShort(address, s);
      } else {
         putShortByByte(address, s);
      }

   }

   public static void setInt(long address, int i) {
      if(Architecture.IS_UNALIGNED) {
         UnsafeAccess.UNSAFE.putInt(address, i);
      } else {
         putIntByByte(address, i);
      }

   }

   public static void setLong(long address, long l) {
      if(Architecture.IS_UNALIGNED) {
         UnsafeAccess.UNSAFE.putLong(address, l);
      } else {
         putLongByByte(address, l);
      }

   }

   public static byte getByte(long address) {
      return UnsafeAccess.UNSAFE.getByte(address);
   }

   public static int getUnsignedShort(long address) {
      return (Architecture.IS_UNALIGNED?UnsafeAccess.UNSAFE.getShort(address):getShortByByte((Object)null, address, BIG_ENDIAN)) & '\uffff';
   }

   public static int getInt(long address) {
      return Architecture.IS_UNALIGNED?UnsafeAccess.UNSAFE.getInt((Object)null, address):getIntByByte((Object)null, address, BIG_ENDIAN);
   }

   public static long getLong(long address) {
      return Architecture.IS_UNALIGNED?UnsafeAccess.UNSAFE.getLong(address):getLongByByte((Object)null, address, BIG_ENDIAN);
   }

   public static long getLongByByte(Object o, long address, boolean bigEndian) {
      return bigEndian?(long)UnsafeAccess.UNSAFE.getByte(o, address) << 56 | ((long)UnsafeAccess.UNSAFE.getByte(o, address + 1L) & 255L) << 48 | ((long)UnsafeAccess.UNSAFE.getByte(o, address + 2L) & 255L) << 40 | ((long)UnsafeAccess.UNSAFE.getByte(o, address + 3L) & 255L) << 32 | ((long)UnsafeAccess.UNSAFE.getByte(o, address + 4L) & 255L) << 24 | ((long)UnsafeAccess.UNSAFE.getByte(o, address + 5L) & 255L) << 16 | ((long)UnsafeAccess.UNSAFE.getByte(o, address + 6L) & 255L) << 8 | (long)UnsafeAccess.UNSAFE.getByte(o, address + 7L) & 255L:(long)UnsafeAccess.UNSAFE.getByte(o, address + 7L) << 56 | ((long)UnsafeAccess.UNSAFE.getByte(o, address + 6L) & 255L) << 48 | ((long)UnsafeAccess.UNSAFE.getByte(o, address + 5L) & 255L) << 40 | ((long)UnsafeAccess.UNSAFE.getByte(o, address + 4L) & 255L) << 32 | ((long)UnsafeAccess.UNSAFE.getByte(o, address + 3L) & 255L) << 24 | ((long)UnsafeAccess.UNSAFE.getByte(o, address + 2L) & 255L) << 16 | ((long)UnsafeAccess.UNSAFE.getByte(o, address + 1L) & 255L) << 8 | (long)UnsafeAccess.UNSAFE.getByte(o, address) & 255L;
   }

   public static int getIntByByte(Object o, long address, boolean bigEndian) {
      return bigEndian?UnsafeAccess.UNSAFE.getByte(o, address) << 24 | (UnsafeAccess.UNSAFE.getByte(o, address + 1L) & 255) << 16 | (UnsafeAccess.UNSAFE.getByte(o, address + 2L) & 255) << 8 | UnsafeAccess.UNSAFE.getByte(o, address + 3L) & 255:UnsafeAccess.UNSAFE.getByte(o, address + 3L) << 24 | (UnsafeAccess.UNSAFE.getByte(o, address + 2L) & 255) << 16 | (UnsafeAccess.UNSAFE.getByte(o, address + 1L) & 255) << 8 | UnsafeAccess.UNSAFE.getByte(o, address) & 255;
   }

   public static short getShortByByte(Object o, long address, boolean bigEndian) {
      return bigEndian?(short)(UnsafeAccess.UNSAFE.getByte(o, address) << 8 | UnsafeAccess.UNSAFE.getByte(o, address + 1L) & 255):(short)(UnsafeAccess.UNSAFE.getByte(o, address + 1L) << 8 | UnsafeAccess.UNSAFE.getByte(o, address) & 255);
   }

   public static void putLongByByte(long address, long value) {
      if(BIG_ENDIAN) {
         UnsafeAccess.UNSAFE.putByte(address, (byte)((int)(value >> 56)));
         UnsafeAccess.UNSAFE.putByte(address + 1L, (byte)((int)(value >> 48)));
         UnsafeAccess.UNSAFE.putByte(address + 2L, (byte)((int)(value >> 40)));
         UnsafeAccess.UNSAFE.putByte(address + 3L, (byte)((int)(value >> 32)));
         UnsafeAccess.UNSAFE.putByte(address + 4L, (byte)((int)(value >> 24)));
         UnsafeAccess.UNSAFE.putByte(address + 5L, (byte)((int)(value >> 16)));
         UnsafeAccess.UNSAFE.putByte(address + 6L, (byte)((int)(value >> 8)));
         UnsafeAccess.UNSAFE.putByte(address + 7L, (byte)((int)value));
      } else {
         UnsafeAccess.UNSAFE.putByte(address + 7L, (byte)((int)(value >> 56)));
         UnsafeAccess.UNSAFE.putByte(address + 6L, (byte)((int)(value >> 48)));
         UnsafeAccess.UNSAFE.putByte(address + 5L, (byte)((int)(value >> 40)));
         UnsafeAccess.UNSAFE.putByte(address + 4L, (byte)((int)(value >> 32)));
         UnsafeAccess.UNSAFE.putByte(address + 3L, (byte)((int)(value >> 24)));
         UnsafeAccess.UNSAFE.putByte(address + 2L, (byte)((int)(value >> 16)));
         UnsafeAccess.UNSAFE.putByte(address + 1L, (byte)((int)(value >> 8)));
         UnsafeAccess.UNSAFE.putByte(address, (byte)((int)value));
      }

   }

   public static void putIntByByte(long address, int value) {
      if(BIG_ENDIAN) {
         UnsafeAccess.UNSAFE.putByte(address, (byte)(value >> 24));
         UnsafeAccess.UNSAFE.putByte(address + 1L, (byte)(value >> 16));
         UnsafeAccess.UNSAFE.putByte(address + 2L, (byte)(value >> 8));
         UnsafeAccess.UNSAFE.putByte(address + 3L, (byte)value);
      } else {
         UnsafeAccess.UNSAFE.putByte(address + 3L, (byte)(value >> 24));
         UnsafeAccess.UNSAFE.putByte(address + 2L, (byte)(value >> 16));
         UnsafeAccess.UNSAFE.putByte(address + 1L, (byte)(value >> 8));
         UnsafeAccess.UNSAFE.putByte(address, (byte)value);
      }

   }

   public static void putShortByByte(long address, short value) {
      if(BIG_ENDIAN) {
         UnsafeAccess.UNSAFE.putByte(address, (byte)(value >> 8));
         UnsafeAccess.UNSAFE.putByte(address + 1L, (byte)value);
      } else {
         UnsafeAccess.UNSAFE.putByte(address + 1L, (byte)(value >> 8));
         UnsafeAccess.UNSAFE.putByte(address, (byte)value);
      }

   }

   public static int pageSize() {
      return UnsafeAccess.UNSAFE.pageSize();
   }

   public static long allocate(long size) {
      return Native.malloc(size);
   }

   public static void free(long peer) {
      Native.free(peer);
   }

   public static void fill(long address, long count, byte b) {
      UnsafeAccess.UNSAFE.setMemory(address, count, b);
   }

   static {
      BIG_ENDIAN = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);
   }
}
