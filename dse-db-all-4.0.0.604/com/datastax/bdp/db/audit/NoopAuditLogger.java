package com.datastax.bdp.db.audit;

import io.reactivex.Completable;
import java.util.Collections;
import java.util.List;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.service.QueryState;

final class NoopAuditLogger implements IAuditLogger {
   NoopAuditLogger() {
   }

   public List<AuditableEvent> getEvents(CQLStatement statement, String queryString, QueryState queryState, QueryOptions queryOptions, List<ColumnSpecification> boundNames) {
      return Collections.emptyList();
   }

   public boolean isEnabled() {
      return false;
   }

   public List<AuditableEvent> getEvents(BatchStatement batch, QueryState queryState, BatchQueryOptions queryOptions) {
      return Collections.emptyList();
   }

   public List<AuditableEvent> getEventsForPrepare(CQLStatement statement, String queryString, QueryState queryState) {
      return Collections.emptyList();
   }

   public Completable logEvents(List<AuditableEvent> events) {
      return Completable.complete();
   }

   public Completable logEvent(AuditableEvent event) {
      return Completable.complete();
   }

   public Completable logFailedQuery(String queryString, QueryState state, Throwable e) {
      return Completable.complete();
   }

   public Completable logFailedQuery(List<AuditableEvent> events, Throwable e) {
      return Completable.complete();
   }
}
