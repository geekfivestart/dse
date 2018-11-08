package org.apache.cassandra.auth.permission;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.enums.Domains;
import org.apache.cassandra.auth.enums.PartitionedEnum;
import org.apache.cassandra.auth.enums.PartitionedEnumSet;

public class Permissions {
   private static final Domains<Permission> domains;

   public Permissions() {
   }

   public static Permission permission(String domain, String name) {
      Permission permission = (Permission)domains.get(domain, name);
      if(permission == null) {
         throw new IllegalArgumentException("Unknown permission: " + domain + "." + name);
      } else {
         return permission;
      }
   }

   public static Permission permission(String fullName) {
      int delim = fullName.indexOf(46);
      String domain = delim == -1?CorePermission.getDomain():fullName.substring(0, delim);
      String name = fullName.substring(delim + 1);
      return permission(domain, name);
   }

   public static Set<Permission> setOf(Permission... permissions) {
      return PartitionedEnumSet.of(Permission.class, (PartitionedEnum[])permissions);
   }

   public static Set<Permission> setOf(Set<Permission> permissions, Permission permission) {
      PartitionedEnumSet<Permission> set = PartitionedEnumSet.of(Permission.class, (Iterable)permissions);
      set.add((PartitionedEnum)permission);
      return set;
   }

   public static Set<Permission> immutableSetOf(Permission... permissions) {
      return PartitionedEnumSet.immutableSetOf(Permission.class, (PartitionedEnum[])permissions);
   }

   public static Set<Permission> immutableSetOf(Iterable<Permission> permissions) {
      return PartitionedEnumSet.immutableSetOf(Permission.class, permissions);
   }

   public static ImmutableSet<Permission> all() {
      return domains.asSet();
   }

   public static <D extends Enum & Permission> void register(String domain, Class<D> permissions) {
      if(domain.equals(CorePermission.getDomain()) && permissions != CorePermission.class) {
         throw new IllegalArgumentException(String.format("The permission domain '%s' is reserved", new Object[]{CorePermission.getDomain()}));
      } else {
         PartitionedEnum.registerDomainForType(Permission.class, domain, permissions);
      }
   }

   static {
      register(CorePermission.getDomain(), CorePermission.class);
      domains = Domains.getDomains(Permission.class);
   }
}
