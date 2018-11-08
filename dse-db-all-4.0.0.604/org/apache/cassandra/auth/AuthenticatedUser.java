package org.apache.cassandra.auth;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;

public class AuthenticatedUser {
   public static final String SYSTEM_USERNAME = "system";
   public static final AuthenticatedUser SYSTEM_USER = new AuthenticatedUser("system");
   public static final String ANONYMOUS_USERNAME = "anonymous";
   public static final AuthenticatedUser ANONYMOUS_USER = new AuthenticatedUser("anonymous");
   public static final String INPROC_USERNAME = "dse_inproc_user";
   public static final AuthenticatedUser INPROC_USER = new AuthenticatedUser("dse_inproc_user");
   private static Map<RoleResource, Role> internalUserRoles = new IdentityHashMap();
   private final String name;
   private final String authenticatedName;
   private final RoleResource role;

   public static Role maybeGetInternalUserRole(RoleResource role) {
      return (Role)internalUserRoles.get(role);
   }

   public AuthenticatedUser(String name) {
      this(name, name);
   }

   public AuthenticatedUser(String name, String authenticatedName) {
      this.name = name;
      this.authenticatedName = authenticatedName;
      this.role = RoleResource.role(name);
   }

   public String getName() {
      return this.name;
   }

   public String getAuthenticatedName() {
      return this.authenticatedName;
   }

   public RoleResource getPrimaryRole() {
      return this.role;
   }

   public RoleResource getLoginRole() {
      return this.role;
   }

   public boolean isAnonymous() {
      return this == ANONYMOUS_USER || this == INPROC_USER;
   }

   public boolean isInProc() {
      return this == INPROC_USER;
   }

   public boolean isSystem() {
      return this == SYSTEM_USER;
   }

   public String toString() {
      return String.format("#<User %s>", new Object[]{this.name});
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof AuthenticatedUser)) {
         return false;
      } else {
         AuthenticatedUser u = (AuthenticatedUser)o;
         return Objects.equals(this.name, u.name);
      }
   }

   public int hashCode() {
      return Objects.hashCode(this.name);
   }

   static {
      internalUserRoles.put(SYSTEM_USER.getPrimaryRole(), new Role("system", ImmutableSet.of(), true, false, ImmutableMap.of(), ""));
      internalUserRoles.put(ANONYMOUS_USER.getPrimaryRole(), new Role("anonymous", ImmutableSet.of(), false, false, ImmutableMap.of(), ""));
      internalUserRoles.put(INPROC_USER.getPrimaryRole(), new Role("dse_inproc_user", ImmutableSet.of(), false, false, ImmutableMap.of(), ""));
   }
}
