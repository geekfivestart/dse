package com.datastax.bdp.cassandra.audit;

import com.datastax.bdp.db.audit.AuditableEventCategory;
import com.datastax.bdp.db.audit.AuditableEventType;

public enum DseAuditableEventType implements AuditableEventType {
   RESTRICT_ROWS_STATEMENT(AuditableEventCategory.DCL),
   UNRESTRICT_ROWS_STATEMENT(AuditableEventCategory.DCL),
   SOLR_RELOAD_SEARCH_INDEX(AuditableEventCategory.DDL),
   SOLR_REBUILD_SEARCH_STATEMENT(AuditableEventCategory.DDL),
   SOLR_DROP_SEARCH_STATEMENT(AuditableEventCategory.DDL),
   SOLR_GET_RESOURCE(AuditableEventCategory.DDL),
   SOLR_UPDATE_RESOURCE(AuditableEventCategory.DDL),
   SOLR_ALTER_SEARCH_INDEX_STATEMENT(AuditableEventCategory.DDL),
   SOLR_CREATE_SEARCH_INDEX_STATEMENT(AuditableEventCategory.DDL),
   SOLR_UPDATE(AuditableEventCategory.DML),
   SOLR_COMMIT_SEARCH_INDEX_STATEMENT(AuditableEventCategory.DML),
   RPC_CALL_STATEMENT(AuditableEventCategory.QUERY),
   GRAPH_TINKERPOP_TRAVERSAL(AuditableEventCategory.QUERY),
   SOLR_QUERY(AuditableEventCategory.QUERY);

   private final AuditableEventCategory category;

   private DseAuditableEventType(AuditableEventCategory category) {
      this.category = category;
   }

   public AuditableEventCategory getCategory() {
      return this.category;
   }
}
