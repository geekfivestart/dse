package org.apache.cassandra.utils;

public abstract class WrappedRunnable implements Runnable {
   public WrappedRunnable() {
   }

   public final void run() {
      try {
         this.runMayThrow();
      } catch (Exception var2) {
         throw com.google.common.base.Throwables.propagate(var2);
      }
   }

   protected abstract void runMayThrow() throws Exception;
}
