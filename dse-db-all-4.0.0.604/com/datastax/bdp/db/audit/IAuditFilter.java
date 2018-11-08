package com.datastax.bdp.db.audit;

@FunctionalInterface
public interface IAuditFilter {
   boolean accept(AuditableEvent var1);
}
