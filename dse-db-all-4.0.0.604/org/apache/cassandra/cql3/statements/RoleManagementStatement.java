package org.apache.cassandra.cql3.statements;

import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.RoleName;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.QueryState;

public abstract class RoleManagementStatement extends AuthenticationStatement {
   protected final RoleResource role;
   protected final RoleResource grantee;

   public RoleManagementStatement(RoleName name, RoleName grantee) {
      this.role = RoleResource.role(name.getName());
      this.grantee = RoleResource.role(grantee.getName());
   }

   public void checkAccess(QueryState state) {
      try {
         super.checkPermission(state, CorePermission.AUTHORIZE, this.role);
      } catch (UnauthorizedException var3) {
         if(!state.hasGrantPermission(this.role, CorePermission.AUTHORIZE)) {
            throw var3;
         }
      }

   }

   public void validate(QueryState state) throws RequestValidationException {
      state.checkNotAnonymous();
      if(!DatabaseDescriptor.getRoleManager().isExistingRole(this.role)) {
         throw new InvalidRequestException(String.format("%s doesn't exist", new Object[]{this.role.getRoleName()}));
      } else if(!DatabaseDescriptor.getRoleManager().isExistingRole(this.grantee)) {
         throw new InvalidRequestException(String.format("%s doesn't exist", new Object[]{this.grantee.getRoleName()}));
      }
   }
}
