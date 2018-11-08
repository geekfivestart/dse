package org.apache.cassandra.cql3.statements;

import io.reactivex.Single;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

public abstract class AuthenticationStatement extends ParsedStatement implements CQLStatement {
   public AuthenticationStatement() {
   }

   public ParsedStatement.Prepared prepare() {
      return new ParsedStatement.Prepared(this);
   }

   public int getBoundTerms() {
      return 0;
   }

   public Single<ResultMessage> execute(QueryState state, QueryOptions options, long queryStartNanoTime) throws RequestExecutionException, RequestValidationException {
      return this.execute(state);
   }

   public abstract Single<ResultMessage> execute(QueryState var1) throws RequestExecutionException, RequestValidationException;

   public Single<ResultMessage> executeInternal(QueryState state, QueryOptions options) {
      throw new UnsupportedOperationException();
   }

   public StagedScheduler getScheduler() {
      return TPC.ioScheduler();
   }

   public void checkPermission(QueryState state, Permission required, RoleResource resource) throws UnauthorizedException {
      try {
         state.checkPermission(resource, required);
      } catch (UnauthorizedException var5) {
         throw new UnauthorizedException(String.format("User %s does not have sufficient privileges to perform the requested operation", new Object[]{state.getUser().getName()}));
      }
   }
}
