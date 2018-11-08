package org.apache.cassandra.index.transactions;

import io.reactivex.Completable;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.rows.Row;

public interface CleanupTransaction extends IndexTransaction {
   CleanupTransaction NO_OP = new CleanupTransaction() {
      public void start() {
      }

      public void onPartitionDeletion(DeletionTime deletionTime) {
      }

      public void onRowDelete(Row row) {
      }

      public Completable commit() {
         return Completable.complete();
      }
   };

   void onPartitionDeletion(DeletionTime var1);

   void onRowDelete(Row var1);
}
