package org.apache.cassandra.utils;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;
import sun.misc.Unsafe;

public class UnsafeAccess {
   public static final Unsafe UNSAFE = (Unsafe)AccessController.doPrivileged(() -> {
      try {
         Field f = Unsafe.class.getDeclaredField("theUnsafe");
         f.setAccessible(true);
         return f.get((Object)null);
      } catch (IllegalAccessException | NoSuchFieldException var1) {
         throw new Error();
      }
   });

   public UnsafeAccess() {
   }
}
