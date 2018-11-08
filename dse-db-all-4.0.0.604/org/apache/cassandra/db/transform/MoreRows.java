package org.apache.cassandra.db.transform;

import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;

public interface MoreRows<I extends BaseRowIterator<?>> extends MoreContents<I> {
   static default UnfilteredRowIterator extend(UnfilteredRowIterator iterator, MoreRows<? super UnfilteredRowIterator> more) {
      return (UnfilteredRowIterator)Transformation.add(Transformation.mutable(iterator), (MoreContents)more);
   }

   static default UnfilteredRowIterator extend(UnfilteredRowIterator iterator, MoreRows<? super UnfilteredRowIterator> more, RegularAndStaticColumns columns) {
      return (UnfilteredRowIterator)Transformation.add(Transformation.wrapIterator(iterator, columns), (MoreContents)more);
   }

   static default RowIterator extend(RowIterator iterator, MoreRows<? super RowIterator> more) {
      return (RowIterator)Transformation.add(Transformation.mutable(iterator), (MoreContents)more);
   }
}
