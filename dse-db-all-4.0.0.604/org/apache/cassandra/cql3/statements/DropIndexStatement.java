package org.apache.cassandra.cql3.statements;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;
import io.reactivex.Maybe;
import io.reactivex.Single;
import io.reactivex.functions.Function;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.cql3.IndexName;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.KeyspaceNotDefinedException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.messages.ResultMessage;

public class DropIndexStatement extends SchemaAlteringStatement implements TableStatement {
   public final String indexName;
   public final boolean ifExists;

   public DropIndexStatement(IndexName indexName, boolean ifExists) {
      super(indexName.getCfName());
      this.indexName = indexName.getIdx();
      this.ifExists = ifExists;
   }

   public AuditableEventType getAuditEventType() {
      return CoreAuditableEventType.DROP_INDEX;
   }

   public String columnFamily() {
      KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(this.keyspace());
      if(ksm == null) {
         return null;
      } else {
         Optional<TableMetadata> indexedTable = ksm.findIndexedTable(this.indexName);
         return indexedTable.isPresent()?((TableMetadata)indexedTable.get()).name:null;
      }
   }

   public void checkAccess(QueryState state) {
      TableMetadata metadata = this.lookupIndexedTable();
      if(metadata != null) {
         state.checkTablePermission(metadata.keyspace, metadata.name, CorePermission.ALTER);
      }
   }

   public void validate(QueryState state) {
   }

   public Single<ResultMessage> execute(QueryState state, QueryOptions options, long queryStartNanoTime) throws RequestValidationException {
      return this.announceMigration(state, false).map((schemaChangeEvent) -> {
         return new ResultMessage.SchemaChange(schemaChangeEvent);
      }).cast(ResultMessage.class).toSingle(new ResultMessage.Void());
   }

   public Maybe<Event.SchemaChange> announceMigration(QueryState queryState, boolean isLocalOnly) throws InvalidRequestException, ConfigurationException {
      TableMetadata current = this.lookupIndexedTable();
      if(current == null) {
         return Maybe.empty();
      } else {
         TableMetadata updated = current.unbuild().indexes(current.indexes.without(this.indexName)).build();
         Event.SchemaChange event = new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TABLE, current.keyspace, current.name);
         return MigrationManager.announceTableUpdate(updated, isLocalOnly).andThen(Maybe.just(event));
      }
   }

   private TableMetadata lookupIndexedTable() {
      KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(this.keyspace());
      if(ksm == null) {
         throw new KeyspaceNotDefinedException("Keyspace " + this.keyspace() + " does not exist");
      } else {
         return (TableMetadata)ksm.findIndexedTable(this.indexName).orElseGet(() -> {
            if(this.ifExists) {
               return null;
            } else {
               throw RequestValidations.invalidRequest("Index '%s' could not be found in any of the tables of keyspace '%s'", new Object[]{this.indexName, this.keyspace()});
            }
         });
      }
   }
}
