package org.apache.cassandra.index.transactions;

import io.reactivex.Completable;
import org.apache.cassandra.db.rows.Row;

public interface CompactionTransaction extends IndexTransaction {
   CompactionTransaction NO_OP = new CompactionTransaction() {
      public void start() {
      }

      public void onRowMerge(Row merged, Row... versions) {
      }

      public Completable commit() {
         return Completable.complete();
      }
   };

   void onRowMerge(Row var1, Row... var2);
}
