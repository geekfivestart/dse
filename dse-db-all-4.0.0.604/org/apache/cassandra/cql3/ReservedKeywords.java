package org.apache.cassandra.cql3;

import com.google.common.annotations.VisibleForTesting;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public final class ReservedKeywords {
   @VisibleForTesting
   static final String[] reservedKeywords = new String[]{"SELECT", "FROM", "WHERE", "AND", "ENTRIES", "FULL", "INSERT", "UPDATE", "WITH", "LIMIT", "USING", "USE", "SET", "BEGIN", "UNLOGGED", "BATCH", "APPLY", "TRUNCATE", "DELETE", "IN", "CREATE", "KEYSPACE", "SCHEMA", "COLUMNFAMILY", "TABLE", "MATERIALIZED", "VIEW", "INDEX", "ON", "TO", "DROP", "PRIMARY", "INTO", "ALTER", "RENAME", "ADD", "ORDER", "BY", "ASC", "DESC", "ALLOW", "IF", "IS", "GRANT", "OF", "REVOKE", "MODIFY", "AUTHORIZE", "DESCRIBE", "EXECUTE", "NORECURSIVE", "TOKEN", "NULL", "NOT", "NAN", "INFINITY", "OR", "REPLACE", "DEFAULT", "UNSET", "MBEAN", "MBEANS", "FOR", "RESTRICT", "UNRESTRICT"};
   private static final Set<String> reservedSet;

   private ReservedKeywords() {
   }

   public static boolean isReserved(String text) {
      return reservedSet.contains(text.toUpperCase());
   }

   public static boolean addReserved(String text) {
      return reservedSet.add(text.toUpperCase());
   }

   static {
      reservedSet = new CopyOnWriteArraySet(Arrays.asList(reservedKeywords));
   }
}
