package org.apache.cassandra.db.transform;

import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;

final class UnfilteredRows extends BaseRows<Unfiltered, UnfilteredRowIterator> implements UnfilteredRowIterator {
   private RegularAndStaticColumns regularAndStaticColumns;
   private DeletionTime partitionLevelDeletion;

   public UnfilteredRows(UnfilteredRowIterator input) {
      this(input, input.columns());
   }

   public UnfilteredRows(UnfilteredRowIterator input, RegularAndStaticColumns columns) {
      super(input);
      this.regularAndStaticColumns = columns;
      this.partitionLevelDeletion = input.partitionLevelDeletion();
   }

   public void add(Transformation add) {
      super.add(add);
      this.regularAndStaticColumns = add.applyToPartitionColumns(this.regularAndStaticColumns);
      this.partitionLevelDeletion = add.applyToDeletion(this.partitionLevelDeletion);
   }

   public RegularAndStaticColumns columns() {
      return this.regularAndStaticColumns;
   }

   public DeletionTime partitionLevelDeletion() {
      return this.partitionLevelDeletion;
   }

   public EncodingStats stats() {
      return ((UnfilteredRowIterator)this.input).stats();
   }

   public boolean isEmpty() {
      return this.staticRow().isEmpty() && this.partitionLevelDeletion().isLive() && !this.hasNext();
   }
}
