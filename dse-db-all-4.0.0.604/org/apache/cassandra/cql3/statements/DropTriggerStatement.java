package org.apache.cassandra.cql3.statements;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;
import io.reactivex.Maybe;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Triggers;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DropTriggerStatement extends SchemaAlteringStatement implements TableStatement {
   private static final Logger logger = LoggerFactory.getLogger(DropTriggerStatement.class);
   private final String triggerName;
   private final boolean ifExists;

   public DropTriggerStatement(CFName name, String triggerName, boolean ifExists) {
      super(name);
      this.triggerName = triggerName;
      this.ifExists = ifExists;
   }

   public AuditableEventType getAuditEventType() {
      return CoreAuditableEventType.DROP_TRIGGER;
   }

   public void checkAccess(QueryState state) {
      if(DatabaseDescriptor.getAuthenticator().requireAuthentication() && !state.isSuper()) {
         throw new UnauthorizedException("Only superusers are allowed to perform DROP TRIGGER queries");
      }
   }

   public void validate(QueryState state) throws RequestValidationException {
      Schema.instance.validateTable(this.keyspace(), this.columnFamily());
   }

   public Maybe<Event.SchemaChange> announceMigration(QueryState queryState, boolean isLocalOnly) throws ConfigurationException, InvalidRequestException {
      TableMetadata current = Schema.instance.getTableMetadata(this.keyspace(), this.columnFamily());
      Triggers triggers = current.triggers;
      if(!triggers.get(this.triggerName).isPresent()) {
         return this.ifExists?Maybe.empty():this.error(String.format("Trigger %s was not found", new Object[]{this.triggerName}));
      } else {
         logger.info("Dropping trigger with name {}", this.triggerName);
         TableMetadata updated = current.unbuild().triggers(triggers.without(this.triggerName)).build();
         return MigrationManager.announceTableUpdate(updated, isLocalOnly).andThen(Maybe.just(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TABLE, this.keyspace(), this.columnFamily())));
      }
   }
}
