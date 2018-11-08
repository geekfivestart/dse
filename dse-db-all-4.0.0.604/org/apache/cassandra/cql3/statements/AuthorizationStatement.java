package org.apache.cassandra.cql3.statements;

import io.reactivex.Single;
import java.util.function.Supplier;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

public abstract class AuthorizationStatement extends ParsedStatement implements CQLStatement {
   public AuthorizationStatement() {
   }

   public ParsedStatement.Prepared prepare() {
      return new ParsedStatement.Prepared(this);
   }

   public int getBoundTerms() {
      return 0;
   }

   public Single<ResultMessage> execute(QueryState state, QueryOptions options, long queryStartNanoTime) throws RequestValidationException, RequestExecutionException {
      return this.execute(state);
   }

   public abstract Single<ResultMessage> execute(QueryState var1) throws RequestValidationException, RequestExecutionException;

   public Single<ResultMessage> executeInternal(QueryState state, QueryOptions options) {
      throw new UnsupportedOperationException();
   }

   public static IResource maybeCorrectResource(IResource resource, ClientState state) throws InvalidRequestException {
      state.getClass();
      return resource.qualifyWithKeyspace(state::getKeyspace);
   }

   public StagedScheduler getScheduler() {
      return TPC.ioScheduler();
   }
}
