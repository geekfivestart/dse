package com.datastax.bdp.db.audit;

public interface AuditableEventType {
   AuditableEventCategory getCategory();
}
