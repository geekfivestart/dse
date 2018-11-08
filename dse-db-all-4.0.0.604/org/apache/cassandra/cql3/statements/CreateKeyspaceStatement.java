package org.apache.cassandra.cql3.statements;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;
import io.reactivex.Maybe;
import io.reactivex.functions.Function;
import java.util.regex.Pattern;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.DataResource;
import org.apache.cassandra.auth.FunctionResource;
import org.apache.cassandra.auth.GrantMode;
import org.apache.cassandra.auth.IAuthorizer;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;

public class CreateKeyspaceStatement extends SchemaAlteringStatement {
   private static final Pattern PATTERN_WORD_CHARS = Pattern.compile("\\w+");
   private final String name;
   private final KeyspaceAttributes attrs;
   private final boolean ifNotExists;

   public CreateKeyspaceStatement(String name, KeyspaceAttributes attrs, boolean ifNotExists) {
      this.name = name;
      this.attrs = attrs;
      this.ifNotExists = ifNotExists;
   }

   public AuditableEventType getAuditEventType() {
      return CoreAuditableEventType.ADD_KS;
   }

   public String keyspace() {
      return this.name;
   }

   public void checkAccess(QueryState state) {
      state.checkAllKeyspacesPermission(CorePermission.CREATE);
   }

   public void validate(QueryState state) throws RequestValidationException {
      Schema.validateKeyspaceNotSystem(this.name);
      if(!PATTERN_WORD_CHARS.matcher(this.name).matches()) {
         throw new InvalidRequestException(String.format("\"%s\" is not a valid keyspace name", new Object[]{this.name}));
      } else if(this.name.length() > 48) {
         throw new InvalidRequestException(String.format("Keyspace names shouldn't be more than %s characters long (got \"%s\")", new Object[]{Integer.valueOf(48), this.name}));
      } else {
         this.attrs.validate();
         if(this.attrs.getReplicationStrategyClass() == null) {
            throw new ConfigurationException("Missing mandatory replication strategy class");
         } else {
            KeyspaceParams params = this.attrs.asNewKeyspaceParams();
            params.validate(this.name);
            if(params.replication.klass.equals(LocalStrategy.class)) {
               throw new ConfigurationException("Unable to use given strategy class: LocalStrategy is reserved for internal use.");
            }
         }
      }
   }

   public Maybe<Event.SchemaChange> announceMigration(QueryState queryState, boolean isLocalOnly) throws RequestValidationException {
      KeyspaceMetadata ksm = KeyspaceMetadata.create(this.name, this.attrs.asNewKeyspaceParams());
      return MigrationManager.announceNewKeyspace(ksm, isLocalOnly).andThen(Maybe.just(new Event.SchemaChange(Event.SchemaChange.Change.CREATED, this.keyspace()))).onErrorResumeNext((e) -> {
         return e instanceof AlreadyExistsException && this.ifNotExists?Maybe.empty():Maybe.error(e);
      });
   }

   protected void grantPermissionsToCreator(QueryState state) {
      try {
         IAuthorizer authorizer = DatabaseDescriptor.getAuthorizer();
         RoleResource role = RoleResource.role(state.getClientState().getUser().getName());
         DataResource keyspace = DataResource.keyspace(this.keyspace());
         authorizer.grant(AuthenticatedUser.SYSTEM_USER, authorizer.applicablePermissions(keyspace), keyspace, role, new GrantMode[]{GrantMode.GRANT});
         FunctionResource functions = FunctionResource.keyspace(this.keyspace());
         authorizer.grant(AuthenticatedUser.SYSTEM_USER, authorizer.applicablePermissions(functions), functions, role, new GrantMode[]{GrantMode.GRANT});
      } catch (RequestExecutionException var6) {
         throw new RuntimeException(var6);
      }
   }
}
