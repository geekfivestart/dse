package org.apache.cassandra.cql3.restrictions;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.schema.TableMetadata;

public interface ExternalRestriction {
   void addToRowFilter(RowFilter var1, TableMetadata var2, QueryOptions var3);
}
