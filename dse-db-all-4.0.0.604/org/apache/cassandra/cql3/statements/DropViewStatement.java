package org.apache.cassandra.cql3.statements;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;
import io.reactivex.Maybe;
import io.reactivex.functions.Function;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;

public class DropViewStatement extends SchemaAlteringStatement implements TableStatement {
   public final boolean ifExists;

   public DropViewStatement(CFName cf, boolean ifExists) {
      super(cf);
      this.ifExists = ifExists;
   }

   public AuditableEventType getAuditEventType() {
      return CoreAuditableEventType.DROP_VIEW;
   }

   public void checkAccess(QueryState state) {
      TableMetadataRef baseTable = View.findBaseTable(this.keyspace(), this.columnFamily());
      if(baseTable != null) {
         state.checkTablePermission(this.keyspace(), baseTable.name, CorePermission.ALTER);
      }

   }

   public void validate(QueryState state) {
   }

   public Maybe<Event.SchemaChange> announceMigration(QueryState queryState, boolean isLocalOnly) throws InvalidRequestException, ConfigurationException {
      return MigrationManager.announceViewDrop(this.keyspace(), this.columnFamily(), isLocalOnly).andThen(Maybe.just(new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, Event.SchemaChange.Target.TABLE, this.keyspace(), this.columnFamily()))).onErrorResumeNext((e) -> {
         return e instanceof ConfigurationException && this.ifExists?Maybe.empty():Maybe.error(e);
      });
   }
}
