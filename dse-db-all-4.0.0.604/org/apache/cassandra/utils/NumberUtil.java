package org.apache.cassandra.utils;

public class NumberUtil {
   public NumberUtil() {
   }

   public static long consistentAbs(long v) {
      return v == -9223372036854775808L?9223372036854775807L:Math.abs(v);
   }
}
