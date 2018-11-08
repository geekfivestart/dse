package org.apache.cassandra.utils.concurrent;

import java.util.concurrent.CompletableFuture;

public interface OpOrder {
   OpOrder.Group start();

   OpOrder.Barrier newBarrier();

   default void awaitNewBarrier() {
      OpOrder.Barrier barrier = this.newBarrier();
      barrier.issue();
      barrier.await();
   }

   public interface Barrier {
      boolean isAfter(OpOrder.Group var1);

      void issue();

      void markBlocking();

      WaitQueue.Signal register();

      boolean allPriorOpsAreFinished();

      void await();
   }

   public interface Group extends AutoCloseable {
      void close();

      boolean isBlocking();

      CompletableFuture<Void> whenBlocking();
   }
}
