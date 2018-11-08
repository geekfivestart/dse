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
import org.apache.cassandra.schema.TriggerMetadata;
import org.apache.cassandra.schema.Triggers;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.triggers.TriggerExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateTriggerStatement extends SchemaAlteringStatement implements TableStatement {
   private static final Logger logger = LoggerFactory.getLogger(CreateTriggerStatement.class);
   private final String triggerName;
   private final String triggerClass;
   private final boolean ifNotExists;

   public CreateTriggerStatement(CFName name, String triggerName, String clazz, boolean ifNotExists) {
      super(name);
      this.triggerName = triggerName;
      this.triggerClass = clazz;
      this.ifNotExists = ifNotExists;
   }

   public AuditableEventType getAuditEventType() {
      return CoreAuditableEventType.CREATE_TRIGGER;
   }

   public void checkAccess(QueryState state) {
      if(DatabaseDescriptor.getAuthenticator().requireAuthentication() && !state.isSuper()) {
         throw new UnauthorizedException("Only superusers are allowed to perform CREATE TRIGGER queries");
      }
   }

   public void validate(QueryState state) throws RequestValidationException {
      TableMetadata metadata = Schema.instance.validateTable(this.keyspace(), this.columnFamily());
      if(metadata.isView()) {
         throw new InvalidRequestException("Cannot CREATE TRIGGER against a materialized view");
      } else {
         try {
            TriggerExecutor.instance.loadTriggerInstance(this.triggerClass);
         } catch (Exception var4) {
            throw new ConfigurationException(String.format("Trigger class '%s' doesn't exist", new Object[]{this.triggerClass}));
         }
      }
   }

   public Maybe<Event.SchemaChange> announceMigration(QueryState queryState, boolean isLocalOnly) throws ConfigurationException, InvalidRequestException {
      TableMetadata current = Schema.instance.getTableMetadata(this.keyspace(), this.columnFamily());
      Triggers triggers = current.triggers;
      if(triggers.get(this.triggerName).isPresent()) {
         return this.ifNotExists?Maybe.empty():this.error(String.format("Trigger %s already exists", new Object[]{this.triggerName}));
      } else {
         TableMetadata updated = current.unbuild().triggers(triggers.with(TriggerMetadata.create(this.triggerName, this.triggerClass))).build();
         logger.info("Adding trigger with name {} and class {}", this.triggerName, this.triggerClass);
         return MigrationManager.announceTableUpdate(updated, isLocalOnly).andThen(Maybe.just(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TABLE, this.keyspace(), this.columnFamily())));
      }
   }
}
