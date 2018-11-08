package org.apache.cassandra.cql3.restrictions;

import java.util.List;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.schema.ColumnMetadata;

public interface Restriction {
   default boolean isOnToken() {
      return false;
   }

   ColumnMetadata getFirstColumn();

   ColumnMetadata getLastColumn();

   List<ColumnMetadata> getColumnDefs();

   void addFunctionsTo(List<Function> var1);

   boolean hasSupportingIndex(SecondaryIndexManager var1);

   void addRowFilterTo(RowFilter var1, SecondaryIndexManager var2, QueryOptions var3);
}
