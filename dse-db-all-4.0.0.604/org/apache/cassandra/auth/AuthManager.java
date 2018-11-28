package org.apache.cassandra.auth;

import com.google.common.annotations.VisibleForTesting;
import io.reactivex.Single;
import io.reactivex.functions.Function;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import org.apache.cassandra.auth.user.UserRolesAndPermissions;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingVersion;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.utils.FBUtilities;

public final class AuthManager {
   private final IRoleManager roleManager;
   @VisibleForTesting
   final RolesCache rolesCache;
   private final IAuthorizer authorizer;
   @VisibleForTesting
   final PermissionsCache permissionsCache;

   public AuthManager(IRoleManager roleManager, IAuthorizer authorizer) {
      this.rolesCache = new RolesCache(roleManager);
      this.permissionsCache = new PermissionsCache(authorizer);
      this.roleManager = new AuthManager.RoleManagerCacheInvalidator(roleManager, authorizer);
      this.authorizer = new AuthManager.AuthorizerInvalidator(roleManager, authorizer);
   }

   public Single<UserRolesAndPermissions> getUserRolesAndPermissions(AuthenticatedUser user) {
      return user.isInProc()?Single.just(UserRolesAndPermissions.INPROC):(user.isSystem()?Single.just(UserRolesAndPermissions.SYSTEM):(user.isAnonymous()?Single.just(UserRolesAndPermissions.ANONYMOUS):this.getUserRolesAndPermissions(user.getName(), user.getAuthenticatedName(), user.getPrimaryRole())));
   }

   public Single<UserRolesAndPermissions> getUserRolesAndPermissions(String name, String authenticatedName) {
      return this.getUserRolesAndPermissions(name, authenticatedName, RoleResource.role(name));
   }

   public Single<UserRolesAndPermissions> getUserRolesAndPermissions(String name, String authenticatedName, RoleResource primaryRole) {
      return this.rolesCache.getRoles(primaryRole).flatMap((rolePerResource) -> {
         Set<RoleResource> roleResources = rolePerResource.keySet();
         return this.isSuperUser(rolePerResource.values())?Single.just(UserRolesAndPermissions.createSuperUserRolesAndPermissions(name, authenticatedName, roleResources)):(!this.authorizer.requireAuthorization()?Single.just(UserRolesAndPermissions.newNormalUserRoles(name, authenticatedName, roleResources)):this.permissionsCache.getAll(roleResources).flatMap((permissions) -> {
            return Single.just(UserRolesAndPermissions.newNormalUserRolesAndPermissions(name, authenticatedName, roleResources, permissions));
         }));
      });
   }

   Single<String> getCredentials(String username) {
      RoleResource resource = RoleResource.role(username);
      return this.rolesCache.get(resource).map((r) -> {
         return r.hashedPassword;
      });
   }

   public Single<Set<RoleResource>> getRoles(RoleResource primaryRole) {
      return this.rolesCache.getRoles(primaryRole).map((roles) -> {
         return Collections.unmodifiableSet(roles.keySet());
      });
   }

   public Single<Map<IResource, PermissionSets>> getPermissions(RoleResource role) {
      return this.permissionsCache.get(role).map(Collections::unmodifiableMap);
   }

   public Single<Boolean> hasSuperUserStatus(RoleResource role) {
      return this.rolesCache.getRoles(role).map((roles) -> {
         return Boolean.valueOf(this.isSuperUser(roles.values()));
      });
   }

   public Single<Boolean> canLogin(RoleResource role) {
      return DatabaseDescriptor.getAuthenticator().getTransitionalMode().failedAuthenticationMapsToAnonymous()?Single.just(Boolean.valueOf(true)):(!this.roleManager.transitiveRoleLogin()?this.rolesCache.get(role).map((r) -> {
         return Boolean.valueOf(r.canLogin);
      }):this.rolesCache.getRoles(role).map((roles) -> {
         return Boolean.valueOf(this.canLogin((Iterable)roles.values()));
      }));
   }

   private boolean isSuperUser(Iterable<Role> roles) {
      Iterator var2 = roles.iterator();

      Role role;
      do {
         if(!var2.hasNext()) {
            return false;
         }

         role = (Role)var2.next();
      } while(!role.isSuper);

      return true;
   }

   private boolean canLogin(Iterable<Role> roles) {
      Iterator var2 = roles.iterator();

      Role role;
      do {
         if(!var2.hasNext()) {
            return false;
         }

         role = (Role)var2.next();
      } while(!role.canLogin);

      return true;
   }

   public IRoleManager getRoleManager() {
      return this.roleManager;
   }

   public IAuthorizer getAuthorizer() {
      return this.authorizer;
   }

   void handleRoleInvalidation(RoleInvalidation invalidation) {
      this.invalidate(invalidation.roles);
   }

   public void invalidateRoles(Collection<RoleResource> roles) {
      this.invalidate(roles);
      this.pushRoleInvalidation(roles);
   }

   private void pushRoleInvalidation(Collection<RoleResource> roles) {
      RoleInvalidation invalidation = new RoleInvalidation(roles);
      Iterator var3 = Gossiper.instance.getLiveMembers().iterator();

      while(var3.hasNext()) {
         InetAddress endpoint = (InetAddress)var3.next();
         if(!endpoint.equals(FBUtilities.getBroadcastAddress()) && MessagingService.instance().versionAtLeast(endpoint, MessagingVersion.DSE_60)) {
            MessagingService.instance().send(Verbs.AUTH.INVALIDATE.newRequest(endpoint, invalidation));
         }
      }

   }

   @VisibleForTesting
   public void invalidateCaches() {
      this.permissionsCache.invalidate();
      this.rolesCache.invalidate();
   }

   private void invalidate(Collection<RoleResource> roles) {
      if(roles.isEmpty()) {
         this.permissionsCache.invalidate();
         this.rolesCache.invalidate();
      } else {
         Iterator var2 = roles.iterator();

         while(var2.hasNext()) {
            RoleResource role = (RoleResource)var2.next();
            this.permissionsCache.invalidate(role);
            this.rolesCache.invalidate(role);
         }
      }

   }

   private class AuthorizerInvalidator implements IAuthorizer {
      private final IRoleManager roleManager;
      private final IAuthorizer authorizer;

      public AuthorizerInvalidator(IRoleManager roleManager, IAuthorizer authorizer) {
         this.roleManager = roleManager;
         this.authorizer = authorizer;
      }

      public IAuthorizer.TransitionalMode getTransitionalMode() {
         return this.authorizer.getTransitionalMode();
      }

      public <T extends IAuthorizer> T implementation() {
         return (T)this.authorizer;
      }

      public boolean requireAuthorization() {
         return this.authorizer.requireAuthorization();
      }

      public Map<IResource, PermissionSets> allPermissionSets(RoleResource role) {
         return this.authorizer.allPermissionSets(role);
      }

      public Set<Permission> grant(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, RoleResource grantee, GrantMode... grantModes) {
         Set<Permission> granted = this.authorizer.grant(performer, permissions, resource, grantee, grantModes);
         if(!granted.isEmpty()) {
            AuthManager.this.invalidateRoles(Collections.singleton(grantee));
         }

         return granted;
      }

      public Set<Permission> revoke(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, RoleResource revokee, GrantMode... grantModes) {
         Set<Permission> revoked = this.authorizer.revoke(performer, permissions, resource, revokee, grantModes);
         if(!revoked.isEmpty()) {
            AuthManager.this.invalidateRoles(Collections.singleton(revokee));
         }

         return revoked;
      }

      public Set<PermissionDetails> list(Set<Permission> permissions, IResource resource, RoleResource grantee) {
         return this.authorizer.list(permissions, resource, grantee);
      }

      public void revokeAllFrom(RoleResource revokee) {
         this.authorizer.revokeAllFrom(revokee);
         AuthManager.this.invalidateRoles(Collections.singleton(revokee));
      }

      public Set<RoleResource> revokeAllOn(IResource droppedResource) {
         Set<RoleResource> roles = this.authorizer.revokeAllOn(droppedResource);
         if(!roles.isEmpty()) {
            AuthManager.this.invalidateRoles(roles);
         }

         return roles;
      }

      public Set<? extends IResource> protectedResources() {
         return this.authorizer.protectedResources();
      }

      public void validateConfiguration() throws ConfigurationException {
         this.authorizer.validateConfiguration();
      }

      public void setup() {
         this.authorizer.setup();
      }

      public Set<Permission> applicablePermissions(IResource resource) {
         return this.authorizer.applicablePermissions(resource);
      }
   }

   private class RoleManagerCacheInvalidator implements IRoleManager {
      private final IRoleManager roleManager;
      private final IAuthorizer authorizer;

      public RoleManagerCacheInvalidator(IRoleManager roleManager, IAuthorizer authorizer) {
         this.roleManager = roleManager;
         this.authorizer = authorizer;
      }

      public <T extends IRoleManager> T implementation() {
         return (T)this.roleManager;
      }

      public Set<IRoleManager.Option> supportedOptions() {
         return this.roleManager.supportedOptions();
      }

      public Set<IRoleManager.Option> alterableOptions() {
         return this.roleManager.alterableOptions();
      }

      public void createRole(AuthenticatedUser performer, RoleResource role, RoleOptions options) {
         this.roleManager.createRole(performer, role, options);
         AuthManager.this.invalidateRoles(Collections.singleton(role));
      }

      public void dropRole(AuthenticatedUser performer, RoleResource role) {
         Set<RoleResource> roles = new HashSet();
         roles.add(role);
         Set<RoleResource> memberOf = this.roleManager.getRoleMemberOf(role);
         if(memberOf != null) {
            roles.addAll(memberOf);
         }

         this.roleManager.dropRole(performer, role);
         this.authorizer.revokeAllFrom(role);
         this.authorizer.revokeAllOn(role);
         AuthManager.this.invalidateRoles(roles);
      }

      public void alterRole(AuthenticatedUser performer, RoleResource role, RoleOptions options) {
         Set<RoleResource> roles = this.getRoles(role, true);
         this.roleManager.alterRole(performer, role, options);
         AuthManager.this.invalidateRoles(roles);
      }

      public void grantRole(AuthenticatedUser performer, RoleResource role, RoleResource grantee) {
         this.roleManager.grantRole(performer, role, grantee);
         AuthManager.this.invalidateRoles(Collections.singleton(grantee));
      }

      public void revokeRole(AuthenticatedUser performer, RoleResource role, RoleResource revokee) {
         this.roleManager.revokeRole(performer, role, revokee);
         AuthManager.this.invalidateRoles(Collections.singleton(revokee));
      }

      public Set<RoleResource> getRoles(RoleResource grantee, boolean includeInherited) {
         return this.roleManager.getRoles(grantee, includeInherited);
      }

      public Set<RoleResource> getRolesIncludingGrantee(RoleResource grantee, boolean includeInherited) {
         return this.roleManager.getRolesIncludingGrantee(grantee, includeInherited);
      }

      public Set<RoleResource> getRoleMemberOf(RoleResource role) {
         return this.roleManager.getRoleMemberOf(role);
      }

      public Set<RoleResource> getAllRoles() {
         return this.roleManager.getAllRoles();
      }

      public boolean isSuper(RoleResource role) {
         return this.roleManager.isSuper(role);
      }

      public boolean canLogin(RoleResource role) {
         return this.roleManager.canLogin(role);
      }

      public boolean transitiveRoleLogin() {
         return this.roleManager.transitiveRoleLogin();
      }

      public Map<String, String> getCustomOptions(RoleResource role) {
         return this.roleManager.getCustomOptions(role);
      }

      public boolean isExistingRole(RoleResource role) {
         return this.roleManager.isExistingRole(role);
      }

      public Set<RoleResource> filterExistingRoleNames(List<String> roleNames) {
         return this.roleManager.filterExistingRoleNames(roleNames);
      }

      public Role getRoleData(RoleResource role) {
         return this.roleManager.getRoleData(role);
      }

      public Set<? extends IResource> protectedResources() {
         return this.roleManager.protectedResources();
      }

      public void validateConfiguration() throws ConfigurationException {
         this.roleManager.validateConfiguration();
      }

      public Future<?> setup() {
         return this.roleManager.setup();
      }

      public boolean hasSuperuserStatus(RoleResource role) {
         return this.roleManager.hasSuperuserStatus(role);
      }
   }
}
