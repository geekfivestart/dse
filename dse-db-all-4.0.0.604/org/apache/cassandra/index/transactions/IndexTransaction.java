package org.apache.cassandra.index.transactions;

import io.reactivex.Completable;

public interface IndexTransaction {
   void start();

   Completable commit();

   public static enum Type {
      UPDATE,
      COMPACTION,
      CLEANUP;

      private Type() {
      }
   }
}
