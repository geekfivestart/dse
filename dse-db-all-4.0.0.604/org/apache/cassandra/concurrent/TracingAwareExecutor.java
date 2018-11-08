package org.apache.cassandra.concurrent;

import java.util.concurrent.Executor;

public interface TracingAwareExecutor extends Executor {
   void execute(Runnable var1, ExecutorLocals var2);

   default int coreId() {
      return TPC.getNumCores();
   }

   default void execute(Runnable runnable) {
      this.execute(runnable, ExecutorLocals.create());
   }
}
