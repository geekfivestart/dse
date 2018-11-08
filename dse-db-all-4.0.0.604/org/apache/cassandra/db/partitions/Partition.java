package org.apache.cassandra.db.partitions;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.SearchIterator;

public interface Partition extends Iterable<Row> {
   int INITIAL_ROW_CAPACITY = 16;

   TableMetadata metadata();

   DecoratedKey partitionKey();

   DeletionTime partitionLevelDeletion();

   DeletionInfo deletionInfo();

   RegularAndStaticColumns columns();

   EncodingStats stats();

   int rowCount();

   Row lastRow();

   boolean hasRows();

   boolean isEmpty();

   Row getRow(Clustering var1);

   SearchIterator<Clustering, Row> searchIterator(ColumnFilter var1, boolean var2);

   UnfilteredRowIterator unfilteredIterator();

   UnfilteredRowIterator unfilteredIterator(ColumnFilter var1, Slices var2, boolean var3);

   FlowableUnfilteredPartition unfilteredPartition();

   FlowableUnfilteredPartition unfilteredPartition(ColumnFilter var1, Slices var2, boolean var3);
}
