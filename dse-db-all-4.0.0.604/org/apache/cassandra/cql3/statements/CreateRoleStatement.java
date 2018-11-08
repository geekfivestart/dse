package org.apache.cassandra.cql3.statements;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;
import io.reactivex.Single;
import java.util.concurrent.Callable;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.GrantMode;
import org.apache.cassandra.auth.IAuthorizer;
import org.apache.cassandra.auth.RoleOptions;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.RoleName;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

public class CreateRoleStatement extends AuthenticationStatement {
   private final RoleResource role;
   private final RoleOptions opts;
   private final boolean ifNotExists;

   public CreateRoleStatement(RoleName name, RoleOptions options, boolean ifNotExists) {
      this.role = RoleResource.role(name.getName());
      this.opts = options;
      this.ifNotExists = ifNotExists;
   }

   public AuditableEventType getAuditEventType() {
      return CoreAuditableEventType.CREATE_ROLE;
   }

   public void checkAccess(QueryState state) {
      state.checkNotAnonymous();
      if(!this.ifNotExists && DatabaseDescriptor.getRoleManager().isExistingRole(this.role)) {
         throw RequestValidations.invalidRequest("%s already exists", new Object[]{this.role.getRoleName()});
      } else {
         this.checkPermission(state, CorePermission.CREATE, RoleResource.root());
         if(this.opts.getSuperuser().isPresent() && ((Boolean)this.opts.getSuperuser().get()).booleanValue() && !state.isSuper()) {
            throw new UnauthorizedException("Only superusers can create a role with superuser status");
         }
      }
   }

   public void validate(QueryState state) throws RequestValidationException {
      this.opts.validate();
      if(this.role.getRoleName().isEmpty()) {
         throw new InvalidRequestException("Role name can't be an empty string");
      }
   }

   public Single<ResultMessage> execute(QueryState state) throws RequestExecutionException, RequestValidationException {
      return Single.fromCallable(() -> {
         if(this.ifNotExists && DatabaseDescriptor.getRoleManager().isExistingRole(this.role)) {
            return new ResultMessage.Void();
         } else {
            DatabaseDescriptor.getRoleManager().createRole(state.getUser(), this.role, this.opts);
            this.grantPermissionsToCreator(state);
            return new ResultMessage.Void();
         }
      });
   }

   private void grantPermissionsToCreator(QueryState state) {
      if(!state.getUser().isAnonymous()) {
         try {
            IAuthorizer authorizer = DatabaseDescriptor.getAuthorizer();
            RoleResource executor = RoleResource.role(state.getUser().getName());
            authorizer.grant(AuthenticatedUser.SYSTEM_USER, authorizer.applicablePermissions(this.role), this.role, executor, new GrantMode[]{GrantMode.GRANT});
         } catch (UnsupportedOperationException var4) {
            ;
         }
      }

   }
}
