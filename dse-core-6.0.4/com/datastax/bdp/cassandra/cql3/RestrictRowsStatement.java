package com.datastax.bdp.cassandra.cql3;

import com.datastax.bdp.cassandra.audit.DseAuditableEventType;
import com.datastax.bdp.cassandra.auth.RowLevelAccessControlAuthorizer;
import com.datastax.bdp.cassandra.auth.RowLevelAccessControlComponentAuthorizer;
import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.server.SystemInfo;
import io.reactivex.Maybe;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.cql3.statements.SchemaAlteringStatement;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.ViewMetadata;
import org.apache.cassandra.schema.ColumnMetadata.Raw;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.Event.SchemaChange.Change;
import org.apache.cassandra.transport.Event.SchemaChange.Target;

public class RestrictRowsStatement extends SchemaAlteringStatement {
   private final Raw column;

   public RestrictRowsStatement(CFName cfname, Raw column) {
      super(cfname);
      this.column = column;
   }

   public Maybe<SchemaChange> announceMigration(QueryState state, boolean isLocalOnly) throws RequestValidationException {
      TableMetadata metadata = Schema.instance.getTableMetadata(this.cfName.getKeyspace(), this.cfName.getColumnFamily());
      Maybe<SchemaChange> change = Maybe.just(new SchemaChange(Change.UPDATED, Target.TABLE, this.keyspace(), this.columnFamily()));
      if(metadata.isView()) {
         ViewMetadata orig = (ViewMetadata)Schema.instance.getKeyspaceMetadata(metadata.keyspace).views.get(metadata.name).get();
         HashMap<String, ByteBuffer> extensions = new HashMap(orig.metadata.params.extensions);
         extensions.put("DSE_RLACA", ByteBuffer.wrap(this.column.rawText().getBytes()));
         ViewMetadata copy = new ViewMetadata(orig.keyspace, orig.name, orig.baseTableId, orig.baseTableName, orig.includeAllColumns, orig.select, orig.whereClause, orig.metadata.unbuild().extensions(extensions).build());
         return MigrationManager.announceViewUpdate(copy, isLocalOnly).andThen(change);
      } else {
         HashMap<String, ByteBuffer> extensions = new HashMap(metadata.params.extensions);
         extensions.put("DSE_RLACA", ByteBuffer.wrap(this.column.rawText().getBytes()));
         TableMetadata copy = metadata.unbuild().extensions(extensions).build();
         return MigrationManager.announceTableUpdate(copy, isLocalOnly).andThen(change);
      }
   }

   public AuditableEventType getAuditEventType() {
      return DseAuditableEventType.RESTRICT_ROWS_STATEMENT;
   }

   public void checkAccess(QueryState state) throws UnauthorizedException, InvalidRequestException {
      state.checkTablePermission(this.keyspace(), this.columnFamily(), CorePermission.ALTER);
      Set<RowLevelAccessControlComponentAuthorizer> componentAuthorizers = RowLevelAccessControlAuthorizer.getRowLevelAccessControlComponentAuthorizers();
      if(componentAuthorizers != null) {
         Iterator var3 = componentAuthorizers.iterator();

         RowLevelAccessControlComponentAuthorizer authorizer;
         do {
            if(!var3.hasNext()) {
               return;
            }

            authorizer = (RowLevelAccessControlComponentAuthorizer)var3.next();
         } while(authorizer.allowsAccess(this.keyspace(), this.columnFamily()));

         String errorMsg = String.format("Component '%s' may not be used on a table with Row Level Access Control enabled", new Object[]{authorizer.getComponentName()});
         throw new InvalidRequestException(errorMsg);
      }
   }

   public void validate(QueryState state) throws RequestValidationException {
      if(SystemInfo.SYSTEM_KEYSPACES.contains(this.keyspace())) {
         throw new InvalidRequestException("Restrict Rows Statement cannot be used on a System Keyspace");
      } else {
         TableMetadata cfm = Schema.instance.validateTable(this.keyspace(), this.columnFamily());
         ColumnMetadata cd = cfm.getColumn(this.column.getIdentifier(cfm));
         if(cd == null) {
            throw new InvalidRequestException(String.format("Invalid Column '%s'", new Object[]{this.column}));
         } else if(!cd.isPrimaryKeyColumn()) {
            throw new InvalidRequestException("Restrict Rows Statement must be for a Primary Key or a Partition Key column");
         } else if(!(cd.getExactTypeIfKnown(this.keyspace()) instanceof UTF8Type)) {
            throw new InvalidRequestException("Restrict Rows Statement can only be used on a UTF8 Column");
         }
      }
   }
}
