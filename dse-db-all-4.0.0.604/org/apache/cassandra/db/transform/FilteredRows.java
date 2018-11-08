package org.apache.cassandra.db.transform;

import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;

public final class FilteredRows extends BaseRows<Row, BaseRowIterator<?>> implements RowIterator {
   FilteredRows(RowIterator input) {
      super((BaseRowIterator)input);
   }

   FilteredRows(UnfilteredRowIterator input, Filter filter) {
      super((BaseRowIterator)input);
      this.add(filter);
   }

   FilteredRows(Filter filter, UnfilteredRows input) {
      super((BaseRows)input);
      this.add(filter);
   }

   public boolean isEmpty() {
      return this.staticRow().isEmpty() && !this.hasNext();
   }

   public static RowIterator filter(UnfilteredRowIterator iterator, int nowInSecs) {
      return (new Filter(nowInSecs, iterator.metadata().enforceStrictLiveness())).applyToPartition(iterator);
   }
}
