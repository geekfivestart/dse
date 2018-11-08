package org.apache.cassandra.db.rows;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.CloseableIterator;

public interface BaseRowIterator<U extends Unfiltered> extends CloseableIterator<U> {
   TableMetadata metadata();

   boolean isReverseOrder();

   RegularAndStaticColumns columns();

   DecoratedKey partitionKey();

   Row staticRow();

   boolean isEmpty();
}
