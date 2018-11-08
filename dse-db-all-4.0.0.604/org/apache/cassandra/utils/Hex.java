package org.apache.cassandra.utils;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Hex {
   private static final Constructor<String> stringConstructor;
   private static final byte[] charToByte;
   private static final Logger logger;
   static final char[] byteToChar;

   public Hex() {
   }

   public static byte[] hexToBytes(String str) {
      if(str.length() % 2 == 1) {
         throw new NumberFormatException("An hex string representing bytes must have an even length");
      } else {
         byte[] bytes = new byte[str.length() / 2];

         for(int i = 0; i < bytes.length; ++i) {
            byte halfByte1 = charToByte[str.charAt(i * 2)];
            byte halfByte2 = charToByte[str.charAt(i * 2 + 1)];
            if(halfByte1 == -1 || halfByte2 == -1) {
               throw new NumberFormatException("Non-hex characters in " + str);
            }

            bytes[i] = (byte)(halfByte1 << 4 | halfByte2);
         }

         return bytes;
      }
   }

   public static String bytesToHex(byte... bytes) {
      return bytesToHex(bytes, 0, bytes.length);
   }

   public static String bytesToHex(byte[] bytes, int offset, int length) {
      char[] c = new char[length * 2];

      for(int i = 0; i < length; ++i) {
         int bint = bytes[i + offset];
         c[i * 2] = byteToChar[(bint & 240) >> 4];
         c[1 + i * 2] = byteToChar[bint & 15];
      }

      return wrapCharArray(c);
   }

   public static String wrapCharArray(char[] c) {
      if(c == null) {
         return null;
      } else {
         String s = null;
         if(stringConstructor != null) {
            try {
               s = (String)stringConstructor.newInstance(new Object[]{Integer.valueOf(0), Integer.valueOf(c.length), c});
            } catch (InvocationTargetException var4) {
               Throwable cause = var4.getCause();
               logger.error("Underlying string constructor threw an error: {}", cause == null?var4.getMessage():cause.getMessage());
            } catch (Exception var5) {
               JVMStabilityInspector.inspectThrowable(var5);
            }
         }

         return s == null?new String(c):s;
      }
   }

   public static <T> Constructor<T> getProtectedConstructor(Class<T> klass, Class... paramTypes) {
      try {
         Constructor<T> c = klass.getDeclaredConstructor(paramTypes);
         c.setAccessible(true);
         return c;
      } catch (Exception var4) {
         return null;
      }
   }

   static {
      stringConstructor = getProtectedConstructor(String.class, new Class[]{Integer.TYPE, Integer.TYPE, char[].class});
      charToByte = new byte[256];
      logger = LoggerFactory.getLogger(Hex.class);
      byteToChar = new char[16];

      for(char c = 0; c < charToByte.length; ++c) {
         if(c >= 48 && c <= 57) {
            charToByte[c] = (byte)(c - 48);
         } else if(c >= 65 && c <= 70) {
            charToByte[c] = (byte)(c - 65 + 10);
         } else if(c >= 97 && c <= 102) {
            charToByte[c] = (byte)(c - 97 + 10);
         } else {
            charToByte[c] = -1;
         }
      }

      for(int i = 0; i < 16; ++i) {
         byteToChar[i] = Integer.toHexString(i).charAt(0);
      }

   }
}
