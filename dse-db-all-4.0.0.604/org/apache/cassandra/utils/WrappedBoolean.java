package org.apache.cassandra.utils;

public class WrappedBoolean {
   private boolean value;

   public WrappedBoolean(boolean initial) {
      this.value = initial;
   }

   public boolean get() {
      return this.value;
   }

   public void set(boolean value) {
      this.value = value;
   }
}
