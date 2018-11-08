package org.apache.cassandra.db.transform;

import org.apache.cassandra.utils.Throwables;

interface MoreContents<I extends AutoCloseable> extends AutoCloseable {
   I moreContents();

   default boolean closeNonAttachedContents() {
      return true;
   }

   default void close() {
      if(this.closeNonAttachedContents()) {
         Throwable err = null;

         for(AutoCloseable next = this.moreContents(); next != null; next = this.moreContents()) {
            try {
               next.close();
            } catch (Throwable var4) {
               err = Throwables.merge(err, var4);
            }
         }

         Throwables.maybeFail(err);
      }
   }
}
