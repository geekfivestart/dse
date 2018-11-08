package com.datastax.bdp.db.audit;

import com.datastax.bdp.db.audit.cql3.BatchStatementUtils;
import com.google.common.annotations.VisibleForTesting;
import io.reactivex.Completable;
import io.reactivex.internal.operators.completable.CompletableEmpty;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.CQLStatementUtils;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.AlterRoleStatement;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.CreateRoleStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MD5Digest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class AuditLogger implements IAuditLogger {
   private static final Logger logger = LoggerFactory.getLogger(AuditLogger.class);
   private static final Pattern obfuscatePasswordPattern = Pattern.compile("(?i)(PASSWORD\\s*(=\\s*)?)'[^']*'");
   static final int MAX_SIZE = Integer.getInteger("dse.audit.bindVariablesFormatter.maxSize", 65536).intValue();
   static final int MAX_VALUE_SIZE = Integer.getInteger("dse.audit.bindVariablesFormatter.maxValueSize", 16384).intValue();
   @VisibleForTesting
   final IAuditWriter writer;
   private final IAuditFilter filter;

   public AuditLogger(IAuditWriter writer, IAuditFilter filter) {
      this.writer = writer;
      this.filter = filter;
   }

   public static AuditLogger fromConfiguration(AuditLoggingOptions auditLoggingOptions) {
      IAuditWriter writer = getWriterInstance(auditLoggingOptions);
      IAuditFilter filter = AuditFilters.fromConfiguration(auditLoggingOptions);
      return new AuditLogger(writer, filter);
   }

   private static IAuditWriter getWriterInstance(AuditLoggingOptions auditLoggingOptions) {
      String className = System.getProperty("cassandra.audit_writer", auditLoggingOptions.logger);
      if(!className.contains(".")) {
         className = "com.datastax.bdp.db.audit." + className;
      }

      return (IAuditWriter)FBUtilities.construct(className, "audit writer");
   }

   private Completable recordEvent(AuditableEvent event) {
      return this.isEnabled() && this.filter.accept(event)?this.writer.recordEvent(event):Completable.complete();
   }

   public boolean isEnabled() {
      return this.writer.isSetUpComplete();
   }

   public void setup() {
      this.writer.setUp();
      logger.info("Audit logging is enabled with " + this.writer.getClass().getName());
   }

   public Completable logEvents(List<AuditableEvent> events) {
      if(events.isEmpty()) {
         return Completable.complete();
      } else {
         List<Completable> eventsLogged = new ArrayList(events.size());
         Iterator var3 = events.iterator();

         while(var3.hasNext()) {
            AuditableEvent event = (AuditableEvent)var3.next();
            Completable c = this.recordEvent(event);
            if(c != CompletableEmpty.INSTANCE) {
               eventsLogged.add(c);
            }
         }

         Completable result = Completable.concat(eventsLogged);
         if(TPC.isTPCThread()) {
            return result;
         } else {
            return result.observeOn(TPC.ioScheduler());
         }
      }
   }

   public Completable logEvent(AuditableEvent event) {
      Completable result = this.recordEvent(event);
      return TPC.isTPCThread()?result:result.observeOn(TPC.ioScheduler());
   }

   public Completable logFailedQuery(String queryString, QueryState state, Throwable e) {
      if(state.isSystem() && !this.isEnabled()) {
         return Completable.complete();
      } else {
         AuditableEventType type = e instanceof UnauthorizedException?CoreAuditableEventType.UNAUTHORIZED_ATTEMPT:CoreAuditableEventType.REQUEST_FAILURE;
         String operation = this.getOperationFrom(queryString, e);
         return this.recordEvent(new AuditableEvent(state, type, operation));
      }
   }

   public Completable logFailedQuery(List<AuditableEvent> events, Throwable e) {
      if(this.isEnabled() && !events.isEmpty()) {
         AuditableEventType type = e instanceof UnauthorizedException?CoreAuditableEventType.UNAUTHORIZED_ATTEMPT:CoreAuditableEventType.REQUEST_FAILURE;
         List<Completable> results = new ArrayList(events.size());
         Iterator var5 = events.iterator();

         while(var5.hasNext()) {
            AuditableEvent event = (AuditableEvent)var5.next();
            String operation = this.getOperationFrom(event.getOperation(), e);
            AuditableEvent auditable = new AuditableEvent(event, type, operation);
            Completable c = this.recordEvent(auditable);
            if(c != CompletableEmpty.INSTANCE) {
               results.add(c);
            }
         }

         return Completable.concat(results);
      } else {
         return Completable.complete();
      }
   }

   private String getOperationFrom(String query, Throwable e) {
      if(query == null) {
         return e.getLocalizedMessage();
      } else {
         int length = e.getLocalizedMessage().length() + query.length() + 1;
         return (new StringBuilder(length)).append(e.getLocalizedMessage()).append(' ').append(query).toString();
      }
   }

   public List<AuditableEvent> getEvents(CQLStatement statement, String queryString, QueryState queryState, QueryOptions queryOptions, List<ColumnSpecification> boundNames) {
      if(!queryState.isSystem() && this.isEnabled() && !this.isPagingQuery(queryOptions)) {
         if(!(statement instanceof BatchStatement)) {
            return Collections.singletonList(this.getEvent(queryState, statement, queryString, (UUID)null, queryOptions.getValues(), boundNames, queryOptions.getConsistency()));
         } else {
            UUID batchID = UUID.randomUUID();
            List<BatchStatementUtils.Meta> batchStatements = BatchStatementUtils.decomposeBatchStatement(queryString);
            List<ModificationStatement> statements = ((BatchStatement)statement).getStatements();
            List<AuditableEvent> events = new ArrayList(batchStatements.size());
            int i = 0;

            for(int m = batchStatements.size(); i < m; ++i) {
               ModificationStatement stmt = (ModificationStatement)statements.get(i);
               BatchStatementUtils.Meta stmtMeta = (BatchStatementUtils.Meta)batchStatements.get(i);
               events.add(this.getEvent(queryState, stmt, stmtMeta.query, batchID, stmtMeta.getSubList(queryOptions.getValues()), stmtMeta.getSubList(boundNames), queryOptions.getConsistency()));
            }

            return events;
         }
      } else {
         return Collections.emptyList();
      }
   }

   private AuditableEvent getEvent(QueryState queryState, CQLStatement statement, String queryString, UUID batchID, List<ByteBuffer> variables, List<ColumnSpecification> boundNames, ConsistencyLevel consistencyLevel) {
      return new AuditableEvent(queryState, getAuditEventType(statement, queryString), batchID, CQLStatementUtils.getKeyspace(statement), CQLStatementUtils.getTable(statement), getOperation(statement, queryString, variables, boundNames), consistencyLevel);
   }

   public List<AuditableEvent> getEvents(BatchStatement batch, QueryState queryState, BatchQueryOptions queryOptions) {
      if(!queryState.isSystem() && this.isEnabled()) {
         UUID batchId = UUID.randomUUID();
         List<Object> queryOrIdList = queryOptions.getQueryOrIdList();
         List<AuditableEvent> events = new ArrayList(queryOrIdList.size());

         for(int i = 0; i < queryOrIdList.size(); ++i) {
            Object queryOrId = queryOrIdList.get(i);
            if(queryOrId instanceof String) {
               events.add(this.getEvent(queryState, (CQLStatement)batch.getStatements().get(i), (String)queryOrId, batchId, queryOptions.forStatement(i).getValues(), queryOptions.forStatement(i).getColumnSpecifications(), queryOptions.getConsistency()));
            } else {
               if(!(queryOrId instanceof MD5Digest)) {
                  throw new IllegalArgumentException("Got unexpected " + queryOrId);
               }

               queryState.getClientState();
               ParsedStatement.Prepared preparedStatement = ClientState.getCQLQueryHandler().getPrepared((MD5Digest)queryOrId);
               if(preparedStatement == null) {
                  logger.warn(String.format("Prepared Statement [id=%s] is null! This usually happens because the KS or CF was dropped between the prepared statement creation and its retrieval from cache", new Object[]{queryOrId.toString()}));
               } else {
                  events.add(this.getEvent(queryState, preparedStatement.statement, preparedStatement.rawCQLStatement, batchId, queryOptions.forStatement(i).getValues(), preparedStatement.boundNames, queryOptions.getConsistency()));
               }
            }
         }

         return events;
      } else {
         return Collections.emptyList();
      }
   }

   public List<AuditableEvent> getEventsForPrepare(CQLStatement statement, String queryString, QueryState queryState) {
      if(!queryState.isSystem() && this.isEnabled()) {
         if(!(statement instanceof BatchStatement)) {
            return Collections.singletonList(this.getEventForPrepared(queryState, statement, queryString, (UUID)null));
         } else {
            UUID batchID = UUID.randomUUID();
            List<BatchStatementUtils.Meta> batchStatements = BatchStatementUtils.decomposeBatchStatement(queryString);
            List<ModificationStatement> statements = ((BatchStatement)statement).getStatements();
            List<AuditableEvent> events = new ArrayList(batchStatements.size());
            int i = 0;

            for(int m = batchStatements.size(); i < m; ++i) {
               events.add(this.getEventForPrepared(queryState, (CQLStatement)statements.get(i), ((BatchStatementUtils.Meta)batchStatements.get(i)).query, batchID));
            }

            return events;
         }
      } else {
         return Collections.emptyList();
      }
   }

   private AuditableEvent getEventForPrepared(QueryState queryState, CQLStatement statement, String queryString, UUID batchID) {
      return new AuditableEvent(queryState, CoreAuditableEventType.CQL_PREPARE_STATEMENT, batchID, CQLStatementUtils.getKeyspace(statement), CQLStatementUtils.getTable(statement), queryString, AuditableEvent.NO_CL);
   }

   private boolean isPagingQuery(QueryOptions options) {
      QueryOptions.PagingOptions pagingOptions = options.getPagingOptions();
      boolean pagingQuery = pagingOptions != null && pagingOptions.state() != null;
      return pagingQuery;
   }

   private static String obfuscatePasswordsIfNeeded(CQLStatement stmt, String queryString) {
      if(stmt instanceof CreateRoleStatement || stmt instanceof AlterRoleStatement) {
         queryString = obfuscatePasswordPattern.matcher(queryString).replaceAll("$1'*****'");
      }

      return queryString;
   }

   private static String getOperation(CQLStatement stmt, String queryString, List<ByteBuffer> variables, List<ColumnSpecification> boundNames) {
      if(null != variables && !variables.isEmpty()) {
         int estimatedSize = queryString.length() + 22 * variables.size() + 2;
         StringBuilder builder = (new StringBuilder(estimatedSize)).append(queryString).append(' ');
         appendBindVariables(builder, boundNames, variables);
         return obfuscatePasswordsIfNeeded(stmt, builder.toString());
      } else {
         return obfuscatePasswordsIfNeeded(stmt, queryString);
      }
   }

   private static AuditableEventType getAuditEventType(CQLStatement stmt, String queryString) {
      AuditableEventType type = stmt.getAuditEventType();
      if(null != type) {
         return type;
      } else {
         logger.warn("Encountered a CQL statement I don't know how to log : " + stmt.getClass().getName() + " ( " + queryString + ")");
         return CoreAuditableEventType.UNKNOWN;
      }
   }

   @VisibleForTesting
   static StringBuilder appendBindVariables(StringBuilder builder, List<ColumnSpecification> boundNames, List<ByteBuffer> variables) {
      if(null != boundNames && !boundNames.isEmpty()) {
         int initialLength = builder.length();
         builder.append('[');
         int i = 0;

         for(int m = variables.size(); i < m; ++i) {
            if(builder.length() - initialLength > MAX_SIZE) {
               return builder.append(", ... (capped)]");
            }

            if(i != 0) {
               builder.append(',');
            }

            ColumnSpecification spec = (ColumnSpecification)boundNames.get(i);
            ByteBuffer var = (ByteBuffer)variables.get(i);
            builder.append(spec.name).append('=');
            String value = spec.type.getString(var);
            if(value.length() <= MAX_VALUE_SIZE) {
               builder.append(value);
            } else {
               builder.append(value, 0, MAX_VALUE_SIZE).append("... (truncated, ").append(value.length() - MAX_VALUE_SIZE).append(" chars omitted)");
            }
         }

         return builder.append(']');
      } else {
         return builder.append("[bind variable values unavailable]");
      }
   }
}
