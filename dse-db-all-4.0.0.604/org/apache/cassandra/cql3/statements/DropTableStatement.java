package org.apache.cassandra.cql3.statements;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;
import io.reactivex.Maybe;
import io.reactivex.functions.Function;
import java.util.Iterator;
import java.util.concurrent.Callable;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.ViewMetadata;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;

public class DropTableStatement extends SchemaAlteringStatement implements TableStatement {
   private final boolean ifExists;

   public DropTableStatement(CFName name, boolean ifExists) {
      super(name);
      this.ifExists = ifExists;
   }

   public AuditableEventType getAuditEventType() {
      return CoreAuditableEventType.DROP_CF;
   }

   public void checkAccess(QueryState state) {
      try {
         state.checkTablePermission(this.keyspace(), this.columnFamily(), CorePermission.DROP);
      } catch (InvalidRequestException var3) {
         if(!this.ifExists) {
            throw var3;
         }
      }

   }

   public void validate(QueryState state) {
   }

   public Maybe<Event.SchemaChange> announceMigration(QueryState queryState, boolean isLocalOnly) throws ConfigurationException {
      return Maybe.defer(() -> {
         KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(this.keyspace());
         if(ksm == null) {
            return this.ifExists?Maybe.empty():this.error(String.format("Cannot drop table in unknown keyspace '%s'", new Object[]{this.keyspace()}));
         } else {
            TableMetadata metadata = ksm.getTableOrViewNullable(this.columnFamily());
            if(metadata != null) {
               if(metadata.isView()) {
                  return this.error("Cannot use DROP TABLE on Materialized View");
               }

               boolean rejectDrop = false;
               StringBuilder messageBuilder = new StringBuilder();
               Iterator var6 = ksm.views.iterator();

               while(var6.hasNext()) {
                  ViewMetadata def = (ViewMetadata)var6.next();
                  if(def.baseTableId.equals(metadata.id)) {
                     if(rejectDrop) {
                        messageBuilder.append(',');
                     }

                     rejectDrop = true;
                     messageBuilder.append(def.name);
                  }
               }

               if(rejectDrop) {
                  return this.error(String.format("Cannot drop table when materialized views still depend on it (%s.{%s})", new Object[]{this.keyspace(), messageBuilder.toString()}));
               }
            }

            return MigrationManager.announceTableDrop(this.keyspace(), this.columnFamily(), isLocalOnly).andThen(Maybe.just(new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, Event.SchemaChange.Target.TABLE, this.keyspace(), this.columnFamily()))).onErrorResumeNext((e) -> {
               return e instanceof ConfigurationException && this.ifExists?Maybe.empty():Maybe.error(e);
            });
         }
      });
   }
}
