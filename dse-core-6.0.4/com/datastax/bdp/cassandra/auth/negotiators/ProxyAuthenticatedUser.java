package com.datastax.bdp.cassandra.auth.negotiators;

import com.google.common.base.Objects;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.RoleResource;

public class ProxyAuthenticatedUser extends AuthenticatedUser {
   public final AuthenticatedUser authenticatedUser;
   public final AuthenticatedUser authorizedUser;

   public ProxyAuthenticatedUser(String authenticatedUser, String authorizedUser) {
      this(new AuthenticatedUser(authenticatedUser), new AuthenticatedUser(authorizedUser));
   }

   public ProxyAuthenticatedUser(AuthenticatedUser authenticatedUser, AuthenticatedUser authorizedUser) {
      super((String)null);
      this.authenticatedUser = authenticatedUser;
      this.authorizedUser = authorizedUser;
   }

   public String getName() {
      return this.authorizedUser.getName();
   }

   public String getAuthenticatedName() {
      return this.authenticatedUser.getName();
   }

   public RoleResource getPrimaryRole() {
      return this.authorizedUser.getPrimaryRole();
   }

   public RoleResource getLoginRole() {
      return this.authenticatedUser.getPrimaryRole();
   }

   public boolean isAnonymous() {
      return this.authorizedUser.isAnonymous();
   }

   public boolean isSystem() {
      return this.authorizedUser.isSystem();
   }

   public String toString() {
      return String.format("#<DseUser %s acting as %s>", new Object[]{this.authenticatedUser.getName(), this.authorizedUser.getName()});
   }

   public boolean equals(Object other) {
      if(this == other) {
         return true;
      } else if(other != null && other instanceof ProxyAuthenticatedUser) {
         ProxyAuthenticatedUser o = (ProxyAuthenticatedUser)other;
         return this.authenticatedUser.equals(o.authenticatedUser) && this.authorizedUser.equals(o.authorizedUser);
      } else {
         return false;
      }
   }

   public int hashCode() {
      return Objects.hashCode(new Object[]{this.authenticatedUser, this.authorizedUser});
   }
}
