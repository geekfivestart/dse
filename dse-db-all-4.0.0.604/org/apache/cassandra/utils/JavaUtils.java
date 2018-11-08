package org.apache.cassandra.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JavaUtils {
   private static final Logger logger = LoggerFactory.getLogger(JavaUtils.class);

   public static boolean supportExitOnOutOfMemory(String jreVersion) {
      try {
         int version = parseJavaVersion(jreVersion);
         if(version > 8) {
            return true;
         } else {
            int update = parseUpdateForPre9Versions(jreVersion);
            return version == 7 && update >= 101 || version == 8 && update >= 92;
         }
      } catch (Exception var3) {
         logger.error("Some JRE information could not be retrieved for the JRE version: " + jreVersion, var3);
         return true;
      }
   }

   private static int parseJavaVersion(String jreVersion) {
      String version;
      if(jreVersion.startsWith("1.")) {
         version = jreVersion.substring(2, 3);
      } else {
         int index = jreVersion.indexOf(46);
         if(index < 0) {
            index = jreVersion.indexOf(45);
            if(index < 0) {
               index = jreVersion.length();
            }
         }

         version = jreVersion.substring(0, index);
      }

      return Integer.parseInt(version);
   }

   private static int parseUpdateForPre9Versions(String jreVersion) {
      int dashSeparatorIndex = jreVersion.indexOf(45);
      if(dashSeparatorIndex > 0) {
         jreVersion = jreVersion.substring(0, dashSeparatorIndex);
      }

      int updateSeparatorIndex = jreVersion.indexOf(95);
      return updateSeparatorIndex < 0?0:Integer.parseInt(jreVersion.substring(updateSeparatorIndex + 1));
   }

   private JavaUtils() {
   }
}
