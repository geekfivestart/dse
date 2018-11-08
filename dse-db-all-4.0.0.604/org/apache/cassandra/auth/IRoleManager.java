package org.apache.cassandra.auth;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;

public interface IRoleManager {
   default <T extends IRoleManager> T implementation() {
      return this;
   }

   default <T extends IRoleManager> boolean isImplementationOf(Class<T> implClass) {
      return implClass.isAssignableFrom(this.implementation().getClass());
   }

   Set<IRoleManager.Option> supportedOptions();

   Set<IRoleManager.Option> alterableOptions();

   void createRole(AuthenticatedUser var1, RoleResource var2, RoleOptions var3) throws RequestValidationException, RequestExecutionException;

   void dropRole(AuthenticatedUser var1, RoleResource var2) throws RequestValidationException, RequestExecutionException;

   void alterRole(AuthenticatedUser var1, RoleResource var2, RoleOptions var3) throws RequestValidationException, RequestExecutionException;

   void grantRole(AuthenticatedUser var1, RoleResource var2, RoleResource var3) throws RequestValidationException, RequestExecutionException;

   void revokeRole(AuthenticatedUser var1, RoleResource var2, RoleResource var3) throws RequestValidationException, RequestExecutionException;

   Set<RoleResource> getRoles(RoleResource var1, boolean var2) throws RequestValidationException, RequestExecutionException;

   default Set<RoleResource> getRolesIncludingGrantee(RoleResource grantee, boolean includeInherited) throws RequestValidationException, RequestExecutionException {
      Set<RoleResource> roles = this.getRoles(grantee, includeInherited);
      if(!roles.contains(grantee)) {
         roles.add(grantee);
      }

      return roles;
   }

   Set<RoleResource> getRoleMemberOf(RoleResource var1);

   Set<RoleResource> getAllRoles() throws RequestValidationException, RequestExecutionException;

   boolean isSuper(RoleResource var1);

   boolean canLogin(RoleResource var1);

   default boolean transitiveRoleLogin() {
      return false;
   }

   Map<String, String> getCustomOptions(RoleResource var1);

   boolean isExistingRole(RoleResource var1);

   Set<RoleResource> filterExistingRoleNames(List<String> var1);

   Role getRoleData(RoleResource var1);

   default Map<RoleResource, Role> getRolesData(Iterable<? extends RoleResource> roleResources) {
      Map<RoleResource, Role> r = new HashMap();
      Iterator var3 = roleResources.iterator();

      while(var3.hasNext()) {
         RoleResource roleResource = (RoleResource)var3.next();
         r.put(roleResource, this.getRoleData(roleResource));
      }

      return r;
   }

   Set<? extends IResource> protectedResources();

   void validateConfiguration() throws ConfigurationException;

   Future<?> setup();

   default boolean hasSuperuserStatus(RoleResource role) {
      Iterator var2 = this.getRoles(role, true).iterator();

      RoleResource r;
      do {
         if(!var2.hasNext()) {
            return false;
         }

         r = (RoleResource)var2.next();
      } while(!this.isSuper(r));

      return true;
   }

   public static enum Option {
      SUPERUSER,
      PASSWORD,
      LOGIN,
      OPTIONS;

      private Option() {
      }
   }
}
