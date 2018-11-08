package com.datastax.bdp.db.nodesync;

public enum TracingLevel {
   LOW,
   HIGH;

   private TracingLevel() {
   }

   public static TracingLevel parse(String str) {
      return valueOf(str.toUpperCase());
   }
}
