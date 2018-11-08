package com.datastax.bdp.cassandra.cql3;

import com.datastax.bdp.cassandra.auth.DseAuthorizer;
import com.datastax.bdp.cassandra.auth.RowLevelAccessControlAuthorizer;
import com.datastax.bdp.cassandra.metrics.UserObjectLatencyPlugin;
import com.datastax.bdp.db.audit.AuditableEvent;
import com.datastax.bdp.db.audit.CoreAuditableEventType;
import com.datastax.bdp.db.audit.IAuditLogger;
import com.datastax.bdp.util.DseUtil;
import com.google.common.base.Throwables;
import io.reactivex.Completable;
import io.reactivex.Single;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.concurrent.TPCUtils.WouldBlockException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.CQLStatementUtils;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.UUIDGen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class BaseQueryOperation<S extends CQLStatement, O> implements DseQueryOperationFactory.Operation<S, O> {
   private static final Logger logger = LoggerFactory.getLogger(DseStandardQueryOperationFactory.class);
   public static final IAuditLogger auditLogger = DatabaseDescriptor.getAuditLogger();
   String cql;
   String tableName;
   public S statement;
   public QueryState queryState;
   O options;
   List<ColumnSpecification> boundNames;
   Map<String, ByteBuffer> customPayload;
   long queryStartNanoTime;
   boolean auditStatement;
   List<AuditableEvent> events;

   BaseQueryOperation(String cql, S statement, QueryState queryState, O options, List<ColumnSpecification> boundNames, Map<String, ByteBuffer> customPayload, long queryStartNanoTime, boolean auditStatement) {
      this.cql = cql;
      this.tableName = null;
      this.statement = statement;
      this.queryState = queryState;
      this.options = options;
      this.boundNames = boundNames;
      this.customPayload = customPayload;
      this.queryStartNanoTime = queryStartNanoTime;
      this.auditStatement = auditStatement && auditLogger.isEnabled() && this.auditable();
   }

   public Single<ResultMessage> process() {
      Completable chain = DseUtil.retryingRunnable(this::parse).andThen(DseUtil.retryingRunnable(this::proxyLogin));
      if(RowLevelAccessControlAuthorizer.isEnabled()) {
         chain = chain.andThen(DseUtil.retryingRunnable(this::approveRlac));
      }

      if(this.auditStatement) {
         chain = chain.onErrorResumeNext((e) -> {
            return auditLogger.logFailedQuery(this.getFailureEvent(), e).andThen(Completable.error(e));
         });
         chain = chain.andThen(Completable.defer(() -> {
            return auditLogger.logEvents(this.getEvents());
         }));
      }

      Single<ResultMessage> schain = chain.andThen(Single.defer(this::executeWithTiming).onErrorResumeNext((error) -> {
         return Throwables.getRootCause(error) instanceof WouldBlockException?this.executeWithTiming().subscribeOn(Schedulers.io()).observeOn(TPC.bestTPCScheduler()):Single.error(error);
      }));
      return schain;
   }

   private void proxyLogin() {
      if(this.customPayload != null && this.customPayload.containsKey("ProxyExecute")) {
         try {
            if(!DatabaseDescriptor.getAuthorizer().isImplementationOf(DseAuthorizer.class)) {
               throw new UnauthorizedException("Can't authorize proxy execution without DseAuthorizer.");
            }

            this.queryState = (QueryState)TPCUtils.blockingGet(((DseAuthorizer)DatabaseDescriptor.getAuthorizer().implementation()).getQueryState(this.queryState, this.customPayload, this.statement));
         } catch (NullPointerException | CharacterCodingException var2) {
            logger.warn("Unexpected exception while processing proxy execution request", var2);
            throw new InvalidRequestException(String.format("Couldn't switch to proxy user (%s: %s)", new Object[]{var2.getClass().getName(), var2.getMessage()}));
         }
      }

   }

   void parse() {
   }

   abstract void approveRlac();

   abstract boolean auditable();

   abstract List<AuditableEvent> getEventsFromAuditLogger();

   List<AuditableEvent> getEvents() {
      return this.events != null?this.events:(this.statement == null?this.getFailureEvent():(this.events = this.getEventsFromAuditLogger()));
   }

   private List<AuditableEvent> getFailureEvent() {
      AuditableEvent event = new AuditableEvent(this.queryState, CoreAuditableEventType.REQUEST_FAILURE, this.cql);
      return this.events = Collections.singletonList(event);
   }

   abstract Single<ResultMessage> execute();

   Single<ResultMessage> executeWithTiming() {
      CqlSlowLogPlugin slowLogPlugin = DseQueryHandler.slowLogPlugin;
      UserObjectLatencyPlugin latencyPlugin = DseQueryHandler.latencyPlugin;
      if(!slowLogPlugin.isEnabled() && !latencyPlugin.isEnabled()) {
         return this.execute();
      } else {
         UUID tracingSessionId = Tracing.isTracing()?Tracing.instance.getSessionId():null;
         long startTime = System.nanoTime();
         UUID startUUID = UUIDGen.getTimeUUID();
         return Single.using(() -> {
            return null;
         }, (stat) -> {
            return this.execute();
         }, (unused) -> {
            long duration = TimeUnit.MILLISECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
            slowLogPlugin.maybeRecord(this, startUUID, duration, tracingSessionId);
            latencyPlugin.maybeRecordOperationMetrics(this, duration);
         });
      }
   }

   public boolean isInternal() {
      return this.queryState.getClientState().isInternal;
   }

   public S getStatement() {
      return this.statement;
   }

   public QueryState getQueryState() {
      return this.queryState;
   }

   public O getOptions() {
      return this.options;
   }

   public String getTableName() {
      return this.tableName == null?CQLStatementUtils.getTable(this.statement):this.tableName;
   }

   public String getCql() {
      return this.cql;
   }
}
