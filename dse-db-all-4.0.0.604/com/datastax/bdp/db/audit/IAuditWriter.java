package com.datastax.bdp.db.audit;

import io.reactivex.Completable;

public interface IAuditWriter {
   Completable recordEvent(AuditableEvent var1);

   default boolean isSetUpComplete() {
      return true;
   }

   default void setUp() {
   }
}
