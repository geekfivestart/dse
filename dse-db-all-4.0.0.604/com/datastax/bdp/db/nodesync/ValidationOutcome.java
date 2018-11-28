package com.datastax.bdp.db.nodesync;

import java.util.HashMap;
import java.util.Map;

public enum ValidationOutcome {
   FULL_IN_SYNC(0),
   FULL_REPAIRED(1),
   PARTIAL_IN_SYNC(2),
   PARTIAL_REPAIRED(3),
   UNCOMPLETED(4),
   FAILED(5);

   private final byte code;
   private static final ValidationOutcome[] codeToValue = new ValidationOutcome[values().length];

   private ValidationOutcome(int code) {
      this.code = (byte)code;
   }

   byte code() {
      return this.code;
   }

   static ValidationOutcome fromCode(byte code) {
      if(code >= 0 && code < codeToValue.length) {
         ValidationOutcome outcome = codeToValue[code];
         if(outcome == null) {
            throw new IllegalArgumentException("Code " + code + " has no defined corresponding outcome");
         } else {
            return outcome;
         }
      } else {
         throw new IllegalArgumentException("Invalid (out-of-bound) code " + code);
      }
   }

   static ValidationOutcome completed(boolean isPartial, boolean hadMismatch) {
      return isPartial?(hadMismatch?PARTIAL_REPAIRED:PARTIAL_IN_SYNC):(hadMismatch?FULL_REPAIRED:FULL_IN_SYNC);
   }

   public boolean wasSuccessful() {
      switch (this) {
         case FULL_IN_SYNC:
         case FULL_REPAIRED: {
            return true;
         }
      }
      return false;
   }

   public boolean wasPartial() {
      switch (this) {
         case PARTIAL_IN_SYNC:
         case PARTIAL_REPAIRED: {
            return true;
         }
      }
      return false;
   }

   ValidationOutcome composeWith(ValidationOutcome other) {
      return this.compareTo(other) > 0?this:other;
   }

   public static Map<String, Long> toMap(long[] outcomes) {
      Map<String, Long> m = new HashMap();
      ValidationOutcome[] var2 = values();
      int var3 = var2.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         ValidationOutcome outcome = var2[var4];
         long v = outcomes[outcome.ordinal()];
         if(v != 0L) {
            m.put(outcome.toString(), Long.valueOf(v));
         }
      }

      return m;
   }

   public String toString() {
      return super.toString().toLowerCase();
   }

   static {
      ValidationOutcome[] var0 = values();
      int var1 = var0.length;

      for(int var2 = 0; var2 < var1; ++var2) {
         ValidationOutcome outcome = var0[var2];
         byte c = outcome.code;
         if(codeToValue[c] != null) {
            throw new IllegalStateException(String.format("Code %d is use twice, for both %s and %s", new Object[]{Byte.valueOf(c), codeToValue[c], outcome}));
         }

         codeToValue[c] = outcome;
      }

   }
}
