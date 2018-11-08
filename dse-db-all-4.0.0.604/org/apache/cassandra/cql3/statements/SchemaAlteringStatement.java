package org.apache.cassandra.cql3.statements;

import io.reactivex.Maybe;
import io.reactivex.Single;
import io.reactivex.functions.Function;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.messages.ResultMessage;

public abstract class SchemaAlteringStatement extends CFStatement implements CQLStatement {
   private final boolean isColumnFamilyLevel;

   protected SchemaAlteringStatement() {
      super((CFName)null);
      this.isColumnFamilyLevel = false;
   }

   protected SchemaAlteringStatement(CFName name) {
      super(name);
      this.isColumnFamilyLevel = true;
   }

   public int getBoundTerms() {
      return 0;
   }

   public void prepareKeyspace(ClientState state) throws InvalidRequestException {
      if(this.isColumnFamilyLevel) {
         super.prepareKeyspace(state);
      }

   }

   public ParsedStatement.Prepared prepare() {
      return new ParsedStatement.Prepared(this);
   }

   protected void grantPermissionsToCreator(QueryState state) {
   }

   protected abstract Maybe<Event.SchemaChange> announceMigration(QueryState var1, boolean var2) throws RequestValidationException;

   public Single<ResultMessage> execute(QueryState state, QueryOptions options, long queryStartNanoTime) throws RequestValidationException {
      Maybe<Event.SchemaChange> ce = this.announceMigration(state, false);
      return ce.map((event) -> {
         AuthenticatedUser user = state.getUser();
         if(user != null && !user.isAnonymous() && event.change == Event.SchemaChange.Change.CREATED) {
            try {
               this.grantPermissionsToCreator(state);
            } catch (UnsupportedOperationException var5) {
               ;
            }
         }

         return new ResultMessage.SchemaChange(event);
      }).cast(ResultMessage.class).toSingle(new ResultMessage.Void());
   }

   public Single<ResultMessage> executeInternal(QueryState state, QueryOptions options) {
      return this.announceMigration(state, true).map((s) -> {
         return new ResultMessage.SchemaChange(s);
      }).toSingle(new ResultMessage.Void());
   }

   public StagedScheduler getScheduler() {
      return TPC.ioScheduler();
   }

   protected Maybe<Event.SchemaChange> error(String msg) {
      return Maybe.error(new InvalidRequestException(msg));
   }
}
