package org.apache.cassandra.auth;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public final class Role {
   public static final Role NULL_ROLE = new Role("", ImmutableSet.of(), false, false, ImmutableMap.of(), "");
   public final String name;
   public final ImmutableSet<RoleResource> memberOf;
   public final boolean isSuper;
   public final boolean canLogin;
   public final ImmutableMap<String, String> options;
   final String hashedPassword;

   public Role withOptions(RoleResource role, ImmutableMap<String, String> options) {
      String name = role.getRoleName();
      return new Role(name, this.memberOf, this.isSuper, this.canLogin, options, this.hashedPassword);
   }

   public Role withRoles(RoleResource role, ImmutableSet<RoleResource> memberOf) {
      String name = role.getRoleName();
      return new Role(name, memberOf, this.isSuper, this.canLogin, this.options, this.hashedPassword);
   }

   public Role(String name, ImmutableSet<RoleResource> memberOf, boolean isSuper, boolean canLogin, ImmutableMap<String, String> options, String hashedPassword) {
      this.name = name;
      this.memberOf = memberOf;
      this.isSuper = isSuper;
      this.canLogin = canLogin;
      this.options = ImmutableMap.copyOf(options);
      this.hashedPassword = hashedPassword;
   }

   public String toString() {
      return MoreObjects.toStringHelper(this).add("name", this.name).add("memberOf", this.memberOf).add("isSuper", this.isSuper).add("canLogin", this.canLogin).add("options", this.options).toString();
   }
}
