package com.datastax.bdp.util;

import com.sun.jna.LastErrorException;
import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DSECLibrary {
   private static final Logger logger = LoggerFactory.getLogger(DSECLibrary.class);
   private static final int ENOMEM = 12;
   static final boolean jnaAvailable;

   private static native int mlock(Pointer var0, NativeLong var1) throws LastErrorException;

   private static native String strerror(int var0);

   private static int errno(RuntimeException e) {
      assert e instanceof LastErrorException;

      try {
         return ((LastErrorException)e).getErrorCode();
      } catch (NoSuchMethodError var2) {
         logger.warn("Obsolete version of JNA present; unable to read errno. Upgrade to JNA 3.2.7 or later");
         return 0;
      }
   }

   private DSECLibrary() {
   }

   public static boolean jnaAvailable() {
      return jnaAvailable;
   }

   public static boolean tryMlock(long address, long length) {
      int errNum;
      try {
         Pointer ptr = new Pointer(address);
         errNum = mlock(ptr, new NativeLong(length));
         logger.debug("JNA mlock successful result: {} address: {} length: {}", new Object[]{Integer.valueOf(errNum), Long.valueOf(address), Long.valueOf(length)});
         return true;
      } catch (UnsatisfiedLinkError var6) {
         return false;
      } catch (RuntimeException var7) {
         if(!(var7 instanceof LastErrorException)) {
            throw var7;
         } else {
            if(errno(var7) == 12 && System.getProperty("os.name").toLowerCase().contains("linux")) {
               logger.warn("Unable to lock JVM memory (ENOMEM) at address: {} length: {}. You may need to increase RLIMIT_MEMLOCK for the dse user.", Long.valueOf(address), Long.valueOf(length));
               logger.debug("Exception: ", var7);
            } else {
               errNum = errno(var7);
               logger.warn("Unknown mlock error: {} msg: {}", Integer.valueOf(errNum), strerror(errNum));
               logger.debug("Exception: ", var7);
            }

            return false;
         }
      }
   }

   static {
      boolean success = true;

      try {
         Native.register("c");
      } catch (NoClassDefFoundError var2) {
         logger.warn("JNA not found. Native methods will be disabled.");
         success = false;
      } catch (UnsatisfiedLinkError var3) {
         logger.warn("JNA link failure, one or more native method will be unavailable.");
         logger.debug("JNA link failure details: {}", var3.getMessage());
      } catch (NoSuchMethodError var4) {
         logger.warn("Obsolete version of JNA present; unable to register C library. Upgrade to JNA 3.2.7 or later");
         success = false;
      }

      jnaAvailable = success;
   }
}
