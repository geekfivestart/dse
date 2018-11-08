package com.datastax.bdp.db.audit;

public enum CoreAuditableEventType implements AuditableEventType {
   CQL_SELECT(AuditableEventCategory.QUERY),
   INSERT(AuditableEventCategory.DML),
   BATCH(AuditableEventCategory.DML),
   SET_KS(AuditableEventCategory.DML),
   TRUNCATE(AuditableEventCategory.DML),
   CQL_UPDATE(AuditableEventCategory.DML),
   CQL_DELETE(AuditableEventCategory.DML),
   CQL_PREPARE_STATEMENT(AuditableEventCategory.DML),
   ADD_CF(AuditableEventCategory.DDL),
   DROP_CF(AuditableEventCategory.DDL),
   UPDATE_CF(AuditableEventCategory.DDL),
   ADD_KS(AuditableEventCategory.DDL),
   DROP_KS(AuditableEventCategory.DDL),
   UPDATE_KS(AuditableEventCategory.DDL),
   CREATE_INDEX(AuditableEventCategory.DDL),
   DROP_INDEX(AuditableEventCategory.DDL),
   CREATE_TRIGGER(AuditableEventCategory.DDL),
   DROP_TRIGGER(AuditableEventCategory.DDL),
   CREATE_VIEW(AuditableEventCategory.DDL),
   DROP_VIEW(AuditableEventCategory.DDL),
   UPDATE_VIEW(AuditableEventCategory.DDL),
   CREATE_TYPE(AuditableEventCategory.DDL),
   DROP_TYPE(AuditableEventCategory.DDL),
   UPDATE_TYPE(AuditableEventCategory.DDL),
   CREATE_FUNCTION(AuditableEventCategory.DDL),
   DROP_FUNCTION(AuditableEventCategory.DDL),
   CREATE_AGGREGATE(AuditableEventCategory.DDL),
   DROP_AGGREGATE(AuditableEventCategory.DDL),
   CREATE_ROLE(AuditableEventCategory.DCL),
   ALTER_ROLE(AuditableEventCategory.DCL),
   DROP_ROLE(AuditableEventCategory.DCL),
   LIST_ROLES(AuditableEventCategory.DCL),
   GRANT(AuditableEventCategory.DCL),
   REVOKE(AuditableEventCategory.DCL),
   LIST_PERMISSIONS(AuditableEventCategory.DCL),
   LOGIN(AuditableEventCategory.AUTH),
   LOGIN_ERROR(AuditableEventCategory.AUTH),
   UNAUTHORIZED_ATTEMPT(AuditableEventCategory.AUTH),
   REQUEST_FAILURE(AuditableEventCategory.ERROR),
   UNKNOWN(AuditableEventCategory.UNKNOWN);

   private final AuditableEventCategory category;

   private CoreAuditableEventType(AuditableEventCategory category) {
      this.category = category;
   }

   public AuditableEventCategory getCategory() {
      return this.category;
   }
}
