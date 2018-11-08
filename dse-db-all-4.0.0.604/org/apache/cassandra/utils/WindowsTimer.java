package org.apache.cassandra.utils;

import com.sun.jna.LastErrorException;
import com.sun.jna.Native;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class WindowsTimer {
   private static final Logger logger = LoggerFactory.getLogger(WindowsTimer.class);

   private static native int timeBeginPeriod(int var0) throws LastErrorException;

   private static native int timeEndPeriod(int var0) throws LastErrorException;

   private WindowsTimer() {
   }

   public static void startTimerPeriod(int period) {
      if(period != 0) {
         assert period > 0;

         if(timeBeginPeriod(period) != 0) {
            logger.warn("Failed to set timer to : {}. Performance will be degraded.", Integer.valueOf(period));
         }

      }
   }

   public static void endTimerPeriod(int period) {
      if(period != 0) {
         assert period > 0;

         if(timeEndPeriod(period) != 0) {
            logger.warn("Failed to end accelerated timer period. System timer will remain set to: {} ms.", Integer.valueOf(period));
         }

      }
   }

   static {
      try {
         Native.register("winmm");
      } catch (NoClassDefFoundError var1) {
         logger.warn("JNA not found. winmm.dll cannot be registered. Performance will be negatively impacted on this node.");
      } catch (Exception var2) {
         logger.error("Failed to register winmm.dll. Performance will be negatively impacted on this node.");
      }

   }
}
