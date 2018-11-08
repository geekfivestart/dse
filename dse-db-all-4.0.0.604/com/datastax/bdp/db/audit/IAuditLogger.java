package com.datastax.bdp.db.audit;

import io.reactivex.Completable;
import java.util.List;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.service.QueryState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface IAuditLogger {
   Logger logger = LoggerFactory.getLogger(IAuditLogger.class);

   default void setup() {
   }

   boolean isEnabled();

   List<AuditableEvent> getEvents(CQLStatement var1, String var2, QueryState var3, QueryOptions var4, List<ColumnSpecification> var5);

   List<AuditableEvent> getEvents(BatchStatement var1, QueryState var2, BatchQueryOptions var3);

   List<AuditableEvent> getEventsForPrepare(CQLStatement var1, String var2, QueryState var3);

   Completable logEvents(List<AuditableEvent> var1);

   Completable logEvent(AuditableEvent var1);

   Completable logFailedQuery(String var1, QueryState var2, Throwable var3);

   Completable logFailedQuery(List<AuditableEvent> var1, Throwable var2);

   static default IAuditLogger newInstance(IAuditWriter writer, IAuditFilter filter) {
      return new AuditLogger(writer, filter);
   }

   static default IAuditLogger fromConfiguration(Config config) {
      AuditLoggingOptions auditLoggingOptions = config.audit_logging_options;
      if(!auditLoggingOptions.enabled && System.getProperty("cassandra.audit_writer") == null) {
         logger.info("Audit logging is disabled");
         return new NoopAuditLogger();
      } else {
         return AuditLogger.fromConfiguration(auditLoggingOptions);
      }
   }
}
