package org.apache.cassandra.db.transform;

import org.apache.cassandra.db.rows.BaseRowIterator;

public final class EmptyPartitionsDiscarder extends Transformation<BaseRowIterator<?>> {
   public EmptyPartitionsDiscarder() {
   }

   protected BaseRowIterator applyToPartition(BaseRowIterator iterator) {
      if(iterator.isEmpty()) {
         iterator.close();
         return null;
      } else {
         return iterator;
      }
   }
}
