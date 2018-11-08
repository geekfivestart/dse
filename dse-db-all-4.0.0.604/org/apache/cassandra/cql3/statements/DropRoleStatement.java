package org.apache.cassandra.cql3.statements;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;
import io.reactivex.Single;
import java.util.concurrent.Callable;
import org.apache.cassandra.auth.IRoleManager;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.RoleName;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

public class DropRoleStatement extends AuthenticationStatement {
   private final RoleResource role;
   private final boolean ifExists;

   public DropRoleStatement(RoleName name, boolean ifExists) {
      this.role = RoleResource.role(name.getName());
      this.ifExists = ifExists;
   }

   public AuditableEventType getAuditEventType() {
      return CoreAuditableEventType.DROP_ROLE;
   }

   public void checkAccess(QueryState state) {
      state.checkNotAnonymous();
      IRoleManager roleManager = DatabaseDescriptor.getRoleManager();
      if(!this.ifExists && !roleManager.isExistingRole(this.role)) {
         throw RequestValidations.invalidRequest("%s doesn't exist", new Object[]{this.role.getRoleName()});
      } else if(state != null && state.getUserName().equals(this.role.getRoleName())) {
         throw RequestValidations.invalidRequest("Cannot DROP primary role for current login");
      } else {
         super.checkPermission(state, CorePermission.DROP, this.role);
         if(roleManager.isExistingRole(this.role) && roleManager.hasSuperuserStatus(this.role) && !state.isSuper()) {
            throw new UnauthorizedException("Only superusers can drop a role with superuser status");
         }
      }
   }

   public void validate(QueryState state) throws RequestValidationException {
   }

   public Single<ResultMessage> execute(QueryState state) throws RequestValidationException, RequestExecutionException {
      return Single.fromCallable(() -> {
         if(this.ifExists && !DatabaseDescriptor.getRoleManager().isExistingRole(this.role)) {
            return new ResultMessage.Void();
         } else {
            DatabaseDescriptor.getRoleManager().dropRole(state.getUser(), this.role);
            return new ResultMessage.Void();
         }
      });
   }
}
