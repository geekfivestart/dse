package org.apache.cassandra.cql3.statements;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;
import io.reactivex.Single;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.TruncateException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.flow.RxThreads;

public class TruncateStatement extends CFStatement implements CQLStatement, TableStatement {
   public TruncateStatement(CFName name) {
      super(name);
   }

   public AuditableEventType getAuditEventType() {
      return CoreAuditableEventType.TRUNCATE;
   }

   public int getBoundTerms() {
      return 0;
   }

   public ParsedStatement.Prepared prepare() throws InvalidRequestException {
      return new ParsedStatement.Prepared(this);
   }

   public void checkAccess(QueryState state) {
      state.checkTablePermission(this.keyspace(), this.columnFamily(), CorePermission.MODIFY);
   }

   public void validate(QueryState state) throws InvalidRequestException {
      Schema.instance.validateTable(this.keyspace(), this.columnFamily());
   }

   public Single<ResultMessage> execute(QueryState state, QueryOptions options, long queryStartNanoTime) throws InvalidRequestException, TruncateException {
      return this.executeInternal(state, options);
   }

   public Single<ResultMessage> executeInternal(QueryState state, QueryOptions options) {
      return RxThreads.subscribeOnIo(Single.defer(() -> {
         try {
            TableMetadata metaData = Schema.instance.getTableMetadata(this.keyspace(), this.columnFamily());
            if(metaData.isView()) {
               return Single.error(new InvalidRequestException("Cannot TRUNCATE materialized view directly; must truncate base table instead"));
            }

            StorageProxy.truncateBlocking(this.keyspace(), this.columnFamily());
         } catch (TimeoutException | UnavailableException var2) {
            return Single.error(new TruncateException(var2));
         }

         return Single.just(new ResultMessage.Void());
      }), TPCTaskType.TRUNCATE);
   }

   public StagedScheduler getScheduler() {
      return TPC.ioScheduler();
   }
}
