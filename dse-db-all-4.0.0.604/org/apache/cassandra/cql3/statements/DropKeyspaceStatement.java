package org.apache.cassandra.cql3.statements;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;
import io.reactivex.Maybe;
import io.reactivex.functions.Function;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;

public class DropKeyspaceStatement extends SchemaAlteringStatement {
   private final String keyspace;
   private final boolean ifExists;

   public DropKeyspaceStatement(String keyspace, boolean ifExists) {
      this.keyspace = keyspace;
      this.ifExists = ifExists;
   }

   public AuditableEventType getAuditEventType() {
      return CoreAuditableEventType.DROP_KS;
   }

   public void checkAccess(QueryState state) {
      state.checkKeyspacePermission(this.keyspace, CorePermission.DROP);
   }

   public void validate(QueryState state) throws RequestValidationException {
      Schema.validateKeyspaceNotSystem(this.keyspace);
   }

   public String keyspace() {
      return this.keyspace;
   }

   public Maybe<Event.SchemaChange> announceMigration(QueryState queryState, boolean isLocalOnly) throws ConfigurationException {
      return MigrationManager.announceKeyspaceDrop(this.keyspace, isLocalOnly).andThen(Maybe.just(new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, this.keyspace()))).onErrorResumeNext((e) -> {
         return e instanceof ConfigurationException && this.ifExists?Maybe.empty():Maybe.error(e);
      });
   }
}
