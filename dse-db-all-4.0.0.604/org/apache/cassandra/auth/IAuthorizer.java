package org.apache.cassandra.auth;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;

public interface IAuthorizer {
   default IAuthorizer.TransitionalMode getTransitionalMode() {
      return IAuthorizer.TransitionalMode.DISABLED;
   }

   default <T extends IAuthorizer> T implementation() {
      return (T)this;
   }

   default <T extends IAuthorizer> boolean isImplementationOf(Class<T> implClass) {
      return implClass.isAssignableFrom(this.implementation().getClass());
   }

   default boolean requireAuthorization() {
      return true;
   }

   Map<IResource, PermissionSets> allPermissionSets(RoleResource var1);

   default Map<RoleResource, Map<IResource, PermissionSets>> allPermissionSetsMany(Iterable<? extends RoleResource> roleResources) {
      Map<RoleResource, Map<IResource, PermissionSets>> r = new HashMap();
      Iterator var3 = roleResources.iterator();

      while(var3.hasNext()) {
         RoleResource roleResource = (RoleResource)var3.next();
         r.put(roleResource, this.allPermissionSets(roleResource));
      }

      return r;
   }

   Set<Permission> grant(AuthenticatedUser var1, Set<Permission> var2, IResource var3, RoleResource var4, GrantMode... var5) throws RequestValidationException, RequestExecutionException;

   Set<Permission> revoke(AuthenticatedUser var1, Set<Permission> var2, IResource var3, RoleResource var4, GrantMode... var5) throws RequestValidationException, RequestExecutionException;

   Set<PermissionDetails> list(Set<Permission> var1, IResource var2, RoleResource var3) throws RequestValidationException, RequestExecutionException;

   void revokeAllFrom(RoleResource var1);

   Set<RoleResource> revokeAllOn(IResource var1);

   Set<? extends IResource> protectedResources();

   void validateConfiguration() throws ConfigurationException;

   void setup();

   default Set<Permission> applicablePermissions(IResource resource) {
      return resource.applicablePermissions();
   }

   public static enum TransitionalMode {
      DISABLED {
         public boolean enforcePermissionsOnAuthenticatedUser() {
            return true;
         }

         public boolean enforcePermissionsAgainstAnonymous() {
            return true;
         }
      },
      NORMAL {
         public boolean supportPermission(Permission perm) {
            return perm != CorePermission.AUTHORIZE;
         }

         public boolean supportPermissionForAnonymous(Permission perm) {
            return perm != CorePermission.AUTHORIZE;
         }
      },
      STRICT {
         public boolean enforcePermissionsOnAuthenticatedUser() {
            return true;
         }

         public boolean supportPermissionForAnonymous(Permission perm) {
            return perm != CorePermission.AUTHORIZE;
         }
      };

      private TransitionalMode() {
      }

      public boolean enforcePermissionsOnAuthenticatedUser() {
         return false;
      }

      public boolean enforcePermissionsAgainstAnonymous() {
         return false;
      }

      public boolean supportPermission(Permission perm) {
         return true;
      }

      public boolean supportPermissionForAnonymous(Permission perm) {
         return true;
      }
   }
}
