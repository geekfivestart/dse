package org.apache.cassandra.cql3.statements;

import java.util.Iterator;
import java.util.Set;
import org.apache.cassandra.auth.FunctionResource;
import org.apache.cassandra.auth.GrantMode;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.auth.permission.Permissions;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.RoleName;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.QueryState;
import org.apache.commons.lang3.StringUtils;

public abstract class PermissionsManagementStatement extends AuthorizationStatement {
   protected final Set<Permission> permissions;
   protected IResource resource;
   protected final RoleResource grantee;
   protected final GrantMode grantMode;

   protected PermissionsManagementStatement(Set<Permission> permissions, IResource resource, RoleName grantee, GrantMode grantMode) {
      this.permissions = permissions;
      this.resource = resource;
      this.grantee = RoleResource.role(grantee.getName());
      this.grantMode = grantMode;
   }

   public void validate(QueryState state) throws RequestValidationException {
      if(!DatabaseDescriptor.getAuthorizer().requireAuthorization()) {
         throw RequestValidations.invalidRequest(String.format("%s operation is not supported by the %s if it is not enabled", new Object[]{this.operation(), DatabaseDescriptor.getAuthorizer().implementation().getClass().getSimpleName()}));
      }
   }

   protected abstract String operation();

   public void checkAccess(QueryState state) {
      state.checkNotAnonymous();
      if(!DatabaseDescriptor.getRoleManager().isExistingRole(this.grantee)) {
         throw RequestValidations.invalidRequest("Role %s doesn't exist", new Object[]{this.grantee.getRoleName()});
      } else {
         this.resource = maybeCorrectResource(this.resource, state.getClientState());
         if(this.resource instanceof FunctionResource && "system".equals(((FunctionResource)this.resource).getKeyspace())) {
            throw RequestValidations.invalidRequest("Altering permissions on builtin functions is not supported");
         } else if(!this.resource.exists()) {
            throw RequestValidations.invalidRequest("Resource %s doesn't exist", new Object[]{this.resource});
         } else if(!state.isSuper()) {
            if(this.grantMode == GrantMode.RESTRICT) {
               throw new UnauthorizedException("Only superusers are allowed to RESTRICT/UNRESTRICT");
            } else {
               Set missingPermissions = Permissions.setOf(new Permission[0]);

               try {
                  state.checkPermission(this.resource, CorePermission.AUTHORIZE);
                  Iterator var3 = this.permissions.iterator();

                  while(var3.hasNext()) {
                     Permission p = (Permission)var3.next();

                     try {
                        state.checkPermission(this.resource, p);
                     } catch (UnauthorizedException var6) {
                        missingPermissions.add(p);
                     }
                  }
               } catch (UnauthorizedException var7) {
                  missingPermissions.add(CorePermission.AUTHORIZE);
               }

               if(!missingPermissions.isEmpty()) {
                  if(this.grantMode == GrantMode.GRANTABLE) {
                     throw new UnauthorizedException(String.format("User %s must not grant AUTHORIZE FOR %s permission on %s", new Object[]{state.getUserName(), StringUtils.join(missingPermissions, ", "), this.resource}));
                  }

                  Set<Permission> missingGrantables = Permissions.setOf(new Permission[0]);
                  Iterator var9 = this.permissions.iterator();

                  while(var9.hasNext()) {
                     Permission p = (Permission)var9.next();
                     if(!state.hasGrantPermission(this.resource, p)) {
                        missingGrantables.add(p);
                     }
                  }

                  if(!missingGrantables.isEmpty()) {
                     throw new UnauthorizedException(String.format("User %s has no %s permission nor AUTHORIZE FOR %s permission on %s or any of its parents", new Object[]{state.getUserName(), StringUtils.join(missingPermissions, ", "), StringUtils.join(missingGrantables, ", "), this.resource}));
                  }

                  if(state.hasRole(this.grantee)) {
                     throw new UnauthorizedException(String.format("User %s has grant privilege for %s permission(s) on %s but must not grant/revoke for him/herself", new Object[]{state.getUserName(), StringUtils.join(this.permissions, ", "), this.resource}));
                  }
               }

            }
         }
      }
   }
}
