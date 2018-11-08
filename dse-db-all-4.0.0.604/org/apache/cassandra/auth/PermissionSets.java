package org.apache.cassandra.auth;

import com.google.common.base.MoreObjects;
import java.util.EnumSet;
import java.util.Set;
import org.apache.cassandra.auth.permission.Permissions;

public final class PermissionSets {
   public static final PermissionSets EMPTY = new PermissionSets(Permissions.immutableSetOf(new Permission[0]), Permissions.immutableSetOf(new Permission[0]), Permissions.immutableSetOf(new Permission[0]));
   public final Set<Permission> granted;
   public final Set<Permission> restricted;
   public final Set<Permission> grantables;

   private PermissionSets(Set<Permission> granted, Set<Permission> restricted, Set<Permission> grantables) {
      this.granted = granted;
      this.restricted = restricted;
      this.grantables = grantables;
   }

   public Set<GrantMode> grantModesFor(Permission permission) {
      Set<GrantMode> modes = EnumSet.noneOf(GrantMode.class);
      if(this.granted.contains(permission)) {
         modes.add(GrantMode.GRANT);
      }

      if(this.restricted.contains(permission)) {
         modes.add(GrantMode.RESTRICT);
      }

      if(this.grantables.contains(permission)) {
         modes.add(GrantMode.GRANTABLE);
      }

      return modes;
   }

   public Set<Permission> allContainedPermissions() {
      Set<Permission> all = Permissions.setOf(new Permission[0]);
      all.addAll(this.granted);
      all.addAll(this.restricted);
      all.addAll(this.grantables);
      return all;
   }

   public PermissionSets.Builder unbuild() {
      return (new PermissionSets.Builder()).addGranted(this.granted).addRestricted(this.restricted).addGrantables(this.grantables);
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         PermissionSets that = (PermissionSets)o;
         return !this.granted.equals(that.granted)?false:(!this.restricted.equals(that.restricted)?false:this.grantables.equals(that.grantables));
      } else {
         return false;
      }
   }

   public int hashCode() {
      int result = this.granted.hashCode();
      result = 31 * result + this.restricted.hashCode();
      result = 31 * result + this.grantables.hashCode();
      return result;
   }

   public boolean isEmpty() {
      return this.granted.isEmpty() && this.grantables.isEmpty() && this.restricted.isEmpty();
   }

   public String toString() {
      return MoreObjects.toStringHelper(this).add("granted", this.granted).add("restricted", this.restricted).add("grantables", this.grantables).toString();
   }

   public static PermissionSets.Builder builder() {
      return new PermissionSets.Builder();
   }

   public Set<Permission> effectivePermissions() {
      Set<Permission> result = Permissions.setOf(new Permission[0]);
      result.addAll(this.granted);
      result.removeAll(this.restricted);
      return result;
   }

   public boolean hasEffectivePermission(Permission permission) {
      return this.granted.contains(permission) && !this.restricted.contains(permission);
   }

   public static final class Builder {
      private final Set<Permission> granted;
      private final Set<Permission> restricted;
      private final Set<Permission> grantables;

      private Builder() {
         this.granted = Permissions.setOf(new Permission[0]);
         this.restricted = Permissions.setOf(new Permission[0]);
         this.grantables = Permissions.setOf(new Permission[0]);
      }

      public PermissionSets.Builder addGranted(Set<Permission> granted) {
         this.granted.addAll(granted);
         return this;
      }

      public PermissionSets.Builder addRestricted(Set<Permission> restricted) {
         this.restricted.addAll(restricted);
         return this;
      }

      public PermissionSets.Builder addGrantables(Set<Permission> grantables) {
         this.grantables.addAll(grantables);
         return this;
      }

      public PermissionSets.Builder addGranted(Permission granted) {
         this.granted.add(granted);
         return this;
      }

      public PermissionSets.Builder addRestricted(Permission restricted) {
         this.restricted.add(restricted);
         return this;
      }

      public PermissionSets.Builder addGrantable(Permission grantable) {
         this.grantables.add(grantable);
         return this;
      }

      public PermissionSets.Builder removeGranted(Set<Permission> granted) {
         this.granted.removeAll(granted);
         return this;
      }

      public PermissionSets.Builder removeRestricted(Set<Permission> restricted) {
         this.restricted.removeAll(restricted);
         return this;
      }

      public PermissionSets.Builder removeGrantables(Set<Permission> grantables) {
         this.grantables.removeAll(grantables);
         return this;
      }

      public PermissionSets.Builder removeGranted(Permission granted) {
         this.granted.remove(granted);
         return this;
      }

      public PermissionSets.Builder removeRestricted(Permission restricted) {
         this.restricted.remove(restricted);
         return this;
      }

      public PermissionSets.Builder removeGrantable(Permission grantable) {
         this.grantables.remove(grantable);
         return this;
      }

      public PermissionSets.Builder add(PermissionSets permissionSets) {
         this.granted.addAll(permissionSets.granted);
         this.restricted.addAll(permissionSets.restricted);
         this.grantables.addAll(permissionSets.grantables);
         return this;
      }

      public PermissionSets build() {
         return this.granted.isEmpty() && this.restricted.isEmpty() && this.grantables.isEmpty()?PermissionSets.EMPTY:new PermissionSets(Permissions.immutableSetOf((Iterable)this.granted), Permissions.immutableSetOf((Iterable)this.restricted), Permissions.immutableSetOf((Iterable)this.grantables));
      }
   }
}
