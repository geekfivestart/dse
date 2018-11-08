package org.apache.cassandra.cql3.statements;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;
import io.reactivex.Single;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

public class UseStatement extends ParsedStatement implements CQLStatement, KeyspaceStatement {
   private final String keyspace;

   public UseStatement(String keyspace) {
      this.keyspace = keyspace;
   }

   public String keyspace() {
      return this.keyspace;
   }

   public AuditableEventType getAuditEventType() {
      return CoreAuditableEventType.SET_KS;
   }

   public int getBoundTerms() {
      return 0;
   }

   public ParsedStatement.Prepared prepare() throws InvalidRequestException {
      return new ParsedStatement.Prepared(this);
   }

   public void checkAccess(QueryState state) {
   }

   public void validate(QueryState state) throws InvalidRequestException {
   }

   public Single<ResultMessage> execute(QueryState state, QueryOptions options, long queryStartNanoTime) throws InvalidRequestException {
      state.getClientState().setKeyspace(this.keyspace);
      return Single.just(new ResultMessage.SetKeyspace(this.keyspace));
   }

   public Single<ResultMessage> executeInternal(QueryState state, QueryOptions options) throws InvalidRequestException {
      return this.execute(state, options, System.nanoTime());
   }

   public StagedScheduler getScheduler() {
      return TPC.ioScheduler();
   }
}
