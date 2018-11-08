package org.apache.cassandra.db.rows;

import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.transform.Transformation;

public class WithOnlyQueriedData<I extends BaseRowIterator<?>> extends Transformation<I> {
   private final ColumnFilter filter;

   public WithOnlyQueriedData(ColumnFilter filter) {
      this.filter = filter;
   }

   protected RegularAndStaticColumns applyToPartitionColumns(RegularAndStaticColumns columns) {
      return this.filter.queriedColumns();
   }

   public Row applyToStatic(Row row) {
      return row.withOnlyQueriedData(this.filter);
   }

   protected Row applyToRow(Row row) {
      return row.withOnlyQueriedData(this.filter);
   }
}
