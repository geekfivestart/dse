package com.datastax.bdp.cassandra.cql3;

import com.datastax.bdp.cassandra.audit.DseAuditableEventType;
import com.datastax.bdp.db.audit.AuditableEventType;
import io.reactivex.Maybe;
import java.nio.ByteBuffer;
import java.util.HashMap;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.cql3.statements.SchemaAlteringStatement;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.Event.SchemaChange.Change;
import org.apache.cassandra.transport.Event.SchemaChange.Target;

public class UnRestrictRowsStatement extends SchemaAlteringStatement {
   public UnRestrictRowsStatement(CFName cfname) {
      super(cfname);
   }

   public Maybe<SchemaChange> announceMigration(QueryState state, boolean isLocalOnly) throws RequestValidationException {
      TableMetadata metadata = Schema.instance.getTableMetadata(this.cfName.getKeyspace(), this.cfName.getColumnFamily());
      HashMap<String, ByteBuffer> extensions = new HashMap(metadata.params.extensions);
      extensions.remove("DSE_RLACA");
      TableMetadata copy = metadata.unbuild().extensions(extensions).build();
      Maybe<SchemaChange> change = Maybe.just(new SchemaChange(Change.UPDATED, Target.TABLE, this.keyspace(), this.columnFamily()));
      return MigrationManager.announceTableUpdate(copy, isLocalOnly).andThen(change);
   }

   public AuditableEventType getAuditEventType() {
      return DseAuditableEventType.UNRESTRICT_ROWS_STATEMENT;
   }

   public void checkAccess(QueryState state) throws UnauthorizedException, InvalidRequestException {
      state.checkTablePermission(this.keyspace(), this.columnFamily(), CorePermission.ALTER);
   }

   public void validate(QueryState state) throws RequestValidationException {
      if("system".equals(this.keyspace())) {
         throw new InvalidRequestException("Allowing row level access on the system resource is not supported");
      }
   }
}
