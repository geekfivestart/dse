package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.cassandra.utils.vint.VIntCoding;

public final class TypeSizes {
   private static final int BOOL_SIZE = 1;
   private static final int BYTE_SIZE = 1;
   private static final int SHORT_SIZE = 2;
   private static final int INT_SIZE = 4;
   private static final int LONG_SIZE = 8;
   private static final int UUID_SIZE = 16;

   private TypeSizes() {
   }

   public static int sizeof(String value) {
      int length = encodedUTF8Length(value);

      assert length <= 32767;

      return sizeof((short)length) + length;
   }

   public static int encodedUTF8Length(String st) {
      int strlen = st.length();
      int utflen = 0;

      for(int i = 0; i < strlen; ++i) {
         int c = st.charAt(i);
         if(c >= 1 && c <= 127) {
            ++utflen;
         } else if(c > 2047) {
            utflen += 3;
         } else {
            utflen += 2;
         }
      }

      return utflen;
   }

   public static int sizeofWithShortLength(ByteBuffer value) {
      return sizeof((short)value.remaining()) + value.remaining();
   }

   public static int sizeofWithLength(ByteBuffer value) {
      return sizeof(value.remaining()) + value.remaining();
   }

   public static int sizeofWithVIntLength(ByteBuffer value) {
      return sizeofUnsignedVInt((long)value.remaining()) + value.remaining();
   }

   public static int sizeof(boolean value) {
      return 1;
   }

   public static int sizeof(byte value) {
      return 1;
   }

   public static int sizeof(short value) {
      return 2;
   }

   public static int sizeof(int value) {
      return 4;
   }

   public static int sizeof(long value) {
      return 8;
   }

   public static int sizeof(UUID value) {
      return 16;
   }

   public static int sizeofVInt(long value) {
      return VIntCoding.computeVIntSize(value);
   }

   public static int sizeofUnsignedVInt(long value) {
      return VIntCoding.computeUnsignedVIntSize(value);
   }
}
