package org.apache.cassandra.cql3;

import org.apache.cassandra.cql3.statements.KeyspaceStatement;
import org.apache.cassandra.cql3.statements.TableStatement;

public final class CQLStatementUtils {
   public static String getKeyspace(CQLStatement stmt) {
      return stmt instanceof KeyspaceStatement?((KeyspaceStatement)stmt).keyspace():null;
   }

   public static String getTable(CQLStatement stmt) {
      return stmt instanceof TableStatement?((TableStatement)stmt).columnFamily():null;
   }

   private CQLStatementUtils() {
   }
}
