package org.apache.cassandra.index.transactions;

import io.reactivex.Completable;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.rows.Row;

public interface UpdateTransaction extends IndexTransaction {
   UpdateTransaction NO_OP = new UpdateTransaction() {
      public void start() {
      }

      public void onPartitionDeletion(DeletionTime deletionTime) {
      }

      public void onRangeTombstone(RangeTombstone rangeTombstone) {
      }

      public void onInserted(Row row) {
      }

      public void onUpdated(Row existing, Row updated) {
      }

      public Completable commit() {
         return Completable.complete();
      }
   };

   void onPartitionDeletion(DeletionTime var1);

   void onRangeTombstone(RangeTombstone var1);

   void onInserted(Row var1);

   void onUpdated(Row var1, Row var2);
}
