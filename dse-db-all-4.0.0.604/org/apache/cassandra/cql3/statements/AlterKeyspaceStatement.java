package org.apache.cassandra.cql3.statements;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;
import io.reactivex.Maybe;
import java.util.function.Predicate;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.utils.Streams;

public class AlterKeyspaceStatement extends SchemaAlteringStatement {
   private final String name;
   private final KeyspaceAttributes attrs;

   public AlterKeyspaceStatement(String name, KeyspaceAttributes attrs) {
      this.name = name;
      this.attrs = attrs;
   }

   public AuditableEventType getAuditEventType() {
      return CoreAuditableEventType.UPDATE_KS;
   }

   public String keyspace() {
      return this.name;
   }

   public void checkAccess(QueryState state) {
      state.checkKeyspacePermission(this.name, CorePermission.ALTER);
   }

   public void validate(QueryState state) throws RequestValidationException {
      KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(this.name);
      if(ksm == null) {
         throw new InvalidRequestException("Unknown keyspace " + this.name);
      } else if(SchemaConstants.isLocalSystemKeyspace(ksm.name)) {
         throw new InvalidRequestException("Cannot alter system keyspace");
      } else {
         this.attrs.validate();
         if(this.attrs.getReplicationStrategyClass() == null && !this.attrs.getReplicationOptions().isEmpty()) {
            throw new ConfigurationException("Missing replication strategy class");
         } else {
            if(this.attrs.getReplicationStrategyClass() != null) {
               KeyspaceParams params = this.attrs.asAlteredKeyspaceParams(ksm.params);
               params.validate(this.name);
               if(params.replication.klass.equals(LocalStrategy.class)) {
                  throw new ConfigurationException("Unable to use given strategy class: LocalStrategy is reserved for internal use.");
               }

               this.warnIfIncreasingRF(ksm, params);
            }

         }
      }
   }

   private void warnIfIncreasingRF(KeyspaceMetadata ksm, KeyspaceParams params) {
      AbstractReplicationStrategy oldStrategy = AbstractReplicationStrategy.createReplicationStrategy(ksm.name, ksm.params.replication.klass, StorageService.instance.getTokenMetadata(), DatabaseDescriptor.getEndpointSnitch(), ksm.params.replication.options);
      AbstractReplicationStrategy newStrategy = AbstractReplicationStrategy.createReplicationStrategy(this.keyspace(), params.replication.klass, StorageService.instance.getTokenMetadata(), DatabaseDescriptor.getEndpointSnitch(), params.replication.options);
      if(newStrategy.getReplicationFactor() > oldStrategy.getReplicationFactor()) {
         long tablesWithNodeSync = StorageService.instance.nodeSyncService.isRunning()?Streams.of(ksm.tables).filter((t) -> {
            return t.params.nodeSync.isEnabled(t);
         }).count():0L;
         if(tablesWithNodeSync > 0L) {
            String baseWarn = "After a replication factor increase, data will need to be replicated to achieve the new factor. ";
            if(tablesWithNodeSync == (long)ksm.tables.size()) {
               ClientWarn.instance.warn(baseWarn + "This will be done automatically by NodeSync, but can be prioritized on specific tables by triggering user validations ('nodesync help validation submit').");
            } else {
               ClientWarn.instance.warn(baseWarn + "For the table with NodeSync enabled, NodeSync will do so automatically, though you can prioritize specific tables by triggering user validations ('nodesync help validation submit'). For other tables you need to run a non-incremental repair on all nodes (nodetool repair -pr).");
            }
         } else {
            ClientWarn.instance.warn("When increasing replication factor you need to run a a non-incremental repair on all nodes to distribute the data (nodetool repair -pr).");
         }
      }

   }

   public Maybe<Event.SchemaChange> announceMigration(QueryState queryState, boolean isLocalOnly) throws RequestValidationException {
      KeyspaceMetadata oldKsm = Schema.instance.getKeyspaceMetadata(this.name);
      if(oldKsm == null) {
         return this.error("Unknown keyspace " + this.name);
      } else {
         KeyspaceMetadata newKsm = oldKsm.withSwapped(this.attrs.asAlteredKeyspaceParams(oldKsm.params));
         return MigrationManager.announceKeyspaceUpdate(newKsm, isLocalOnly).andThen(Maybe.just(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, this.keyspace())));
      }
   }
}
