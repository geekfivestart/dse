package com.datastax.bdp.cassandra.auth;

import java.util.Arrays;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.cassandra.auth.DataResource;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.auth.permission.Permissions;

public class DseRowResource implements IResource {
   static final String ROOT_NAME = "rows";
   private static final Set<Permission> permissions;
   private final DataResource wrapped;
   private final String rowTarget;

   private DseRowResource(DataResource wrapped, String rowTarget) {
      this.wrapped = wrapped;
      this.rowTarget = rowTarget;
   }

   public String getName() {
      String wrappedName = this.wrapped.getName();
      return String.format("%s/%s/%s", new Object[]{"rows", this.rowTarget, wrappedName});
   }

   public static DseRowResource fromDataResource(DataResource dataResource, String rowTarget) {
      return new DseRowResource(dataResource, rowTarget);
   }

   public static DseRowResource root(String rowTarget) {
      DataResource wrapped = DataResource.root();
      return new DseRowResource(wrapped, rowTarget);
   }

   public static DseRowResource keyspace(String keyspace, String rowTarget) {
      DataResource wrapped = DataResource.keyspace(keyspace);
      return new DseRowResource(wrapped, rowTarget);
   }

   public static DseRowResource table(String keyspace, String table, String rowTarget) {
      DataResource wrapped = DataResource.table(keyspace, table);
      return new DseRowResource(wrapped, rowTarget);
   }

   public String getRowTarget() {
      return this.rowTarget;
   }

   public IResource getParent() {
      return this.wrapped;
   }

   public boolean hasParent() {
      return true;
   }

   public boolean exists() {
      return this.wrapped.exists();
   }

   public Set<Permission> applicablePermissions() {
      return permissions;
   }

   public String toString() {
      return String.format("'%s' %s IN %s", new Object[]{this.rowTarget, "rows", this.wrapped.toString()});
   }

   public IResource qualifyWithKeyspace(Supplier<String> keyspace) {
      return new DseRowResource((DataResource)this.wrapped.qualifyWithKeyspace(keyspace), this.rowTarget);
   }

   static {
      permissions = Permissions.immutableSetOf(new Permission[]{CorePermission.SELECT, CorePermission.MODIFY});
   }

   public static class Factory implements DseResourceFactory.Factory {
      public Factory() {
      }

      public boolean matches(String name) {
         return name.startsWith("rows");
      }

      public DseRowResource fromName(String name) {
         String[] split = name.split("/");
         if(split.length >= 3 && split[0].equals("rows")) {
            String rowTargetString = split[1];
            String wrappedString = (String)Arrays.stream(split).skip(2L).collect(Collectors.joining("/"));
            DataResource dataResource = DataResource.fromName(wrappedString);
            return new DseRowResource(dataResource, rowTargetString);
         } else {
            throw new IllegalArgumentException(String.format("DseRowResource %s is not a valid row resource", new Object[]{name}));
         }
      }
   }
}
