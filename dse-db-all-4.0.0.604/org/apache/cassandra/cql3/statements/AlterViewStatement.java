package org.apache.cassandra.cql3.statements;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;
import io.reactivex.Maybe;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.db.compaction.DateTieredCompactionStrategy;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.schema.ViewMetadata;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;

public class AlterViewStatement extends SchemaAlteringStatement implements TableStatement {
   private final TableAttributes attrs;

   public AlterViewStatement(CFName name, TableAttributes attrs) {
      super(name);
      this.attrs = attrs;
   }

   public AuditableEventType getAuditEventType() {
      return CoreAuditableEventType.UPDATE_VIEW;
   }

   public void checkAccess(QueryState state) {
      TableMetadataRef baseTable = View.findBaseTable(this.keyspace(), this.columnFamily());
      if(baseTable != null) {
         state.checkTablePermission(this.keyspace(), baseTable.name, CorePermission.ALTER);
      }

   }

   public void validate(QueryState state) {
   }

   public Maybe<Event.SchemaChange> announceMigration(QueryState queryState, boolean isLocalOnly) throws RequestValidationException {
      TableMetadata meta = Schema.instance.validateTable(this.keyspace(), this.columnFamily());
      if(!meta.isView()) {
         return this.error("Cannot use ALTER MATERIALIZED VIEW on Table");
      } else {
         ViewMetadata current = Schema.instance.getView(this.keyspace(), this.columnFamily());
         if(this.attrs == null) {
            return this.error("ALTER MATERIALIZED VIEW WITH invoked, but no parameters found");
         } else {
            this.attrs.validate();
            TableParams params = this.attrs.asAlteredTableParams(current.metadata.params);
            if(params.compaction.klass().equals(DateTieredCompactionStrategy.class) && !current.metadata.params.compaction.klass().equals(DateTieredCompactionStrategy.class)) {
               DateTieredCompactionStrategy.deprecatedWarning(this.keyspace(), this.columnFamily());
            }

            if(params.gcGraceSeconds == 0) {
               return this.error("Cannot alter gc_grace_seconds of a materialized view to 0, since this value is used to TTL undelivered updates. Setting gc_grace_seconds too low might cause undelivered updates to expire before being replayed.");
            } else if(params.defaultTimeToLive > 0) {
               throw new InvalidRequestException("Cannot set or alter default_time_to_live for a materialized view. Data in a materialized view always expire at the same time than the corresponding data in the parent table.");
            } else {
               ViewMetadata updated = current.copy(current.metadata.unbuild().params(params).build());
               return MigrationManager.announceViewUpdate(updated, isLocalOnly).andThen(Maybe.just(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TABLE, this.keyspace(), this.columnFamily())));
            }
         }
      }
   }

   public String toString() {
      return String.format("AlterViewStatement(name=%s)", new Object[]{this.cfName});
   }
}
