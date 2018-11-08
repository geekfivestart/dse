package org.apache.cassandra.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Clock {
   private static final Logger logger = LoggerFactory.getLogger(Clock.class);
   public static Clock instance;

   public Clock() {
   }

   public long nanoTime() {
      return System.nanoTime();
   }

   public long currentTimeMillis() {
      return System.currentTimeMillis();
   }

   static {
      String sclock = System.getProperty("cassandra.clock");
      if(sclock == null) {
         instance = new Clock();
      } else {
         try {
            logger.debug("Using custom clock implementation: {}", sclock);
            instance = (Clock)Class.forName(sclock).newInstance();
         } catch (Exception var2) {
            logger.error(var2.getMessage(), var2);
         }
      }

   }
}
