package org.apache.cassandra.utils;

public interface JVMKiller {
   default void killJVM(Throwable error) {
      this.killJVM(error, false);
   }

   void killJVM(Throwable var1, boolean var2);
}
