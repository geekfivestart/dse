package com.datastax.bdp.db.audit;

import java.util.Locale;
import org.apache.cassandra.exceptions.ConfigurationException;

public enum AuditableEventCategory {
   QUERY,
   DML,
   DDL,
   DCL,
   AUTH,
   ADMIN,
   ERROR,
   UNKNOWN;

   private AuditableEventCategory() {
   }

   public static AuditableEventCategory fromString(String value) {
      try {
         return valueOf(value.toUpperCase(Locale.ENGLISH));
      } catch (IllegalArgumentException var2) {
         throw new ConfigurationException("Unknown audit event category:  " + value, false);
      }
   }
}
