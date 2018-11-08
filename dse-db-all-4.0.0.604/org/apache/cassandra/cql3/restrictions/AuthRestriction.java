package org.apache.cassandra.cql3.restrictions;

import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.service.QueryState;

public interface AuthRestriction {
   void addRowFilterTo(RowFilter var1, QueryState var2);
}
