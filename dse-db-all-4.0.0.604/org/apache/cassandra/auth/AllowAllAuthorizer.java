package org.apache.cassandra.auth;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class AllowAllAuthorizer implements IAuthorizer {
   public AllowAllAuthorizer() {
   }

   public boolean requireAuthorization() {
      return false;
   }

   public Map<IResource, PermissionSets> allPermissionSets(RoleResource role) {
      throw new UnsupportedOperationException();
   }

   public Set<Permission> grant(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, RoleResource to, GrantMode... grantModes) {
      throw new UnsupportedOperationException("GRANT operation is not supported by AllowAllAuthorizer");
   }

   public Set<Permission> revoke(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, RoleResource from, GrantMode... grantModes) {
      throw new UnsupportedOperationException("REVOKE operation is not supported by AllowAllAuthorizer");
   }

   public void revokeAllFrom(RoleResource droppedRole) {
   }

   public Set<RoleResource> revokeAllOn(IResource droppedResource) {
      return Collections.emptySet();
   }

   public Set<PermissionDetails> list(Set<Permission> permissions, IResource resource, RoleResource of) {
      throw new UnsupportedOperationException("LIST PERMISSIONS operation is not supported by AllowAllAuthorizer");
   }

   public Set<IResource> protectedResources() {
      return Collections.emptySet();
   }

   public void validateConfiguration() {
   }

   public void setup() {
   }
}
