package org.apache.cassandra.auth;

import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.auth.permission.Permissions;
import org.apache.cassandra.schema.Schema;
import org.apache.commons.lang3.StringUtils;

public class DataResource implements IResource {
   private static final Set<Permission> TABLE_LEVEL_PERMISSIONS;
   private static final Set<Permission> KEYSPACE_LEVEL_PERMISSIONS;
   private static final String ROOT_NAME = "data";
   private static final DataResource ROOT_RESOURCE;
   private final DataResource.Level level;
   private final String keyspace;
   private final String table;
   private final transient int hash;

   private DataResource(DataResource.Level level, String keyspace, String table) {
      this.level = level;
      this.keyspace = keyspace;
      this.table = table;
      this.hash = Objects.hash(new Object[]{level, keyspace, table});
   }

   public static DataResource root() {
      return ROOT_RESOURCE;
   }

   public static DataResource keyspace(String keyspace) {
      return new DataResource(DataResource.Level.KEYSPACE, keyspace, (String)null);
   }

   public static DataResource table(String keyspace, String table) {
      return new DataResource(DataResource.Level.TABLE, keyspace, table);
   }

   public static DataResource fromName(String name) {
      String[] parts = StringUtils.split(name, '/');
      if(parts[0].equals("data") && parts.length <= 3) {
         return parts.length == 1?root():(parts.length == 2?keyspace(parts[1]):table(parts[1], parts[2]));
      } else {
         throw new IllegalArgumentException(String.format("%s is not a valid data resource name", new Object[]{name}));
      }
   }

   public String getName() {
      switch(null.$SwitchMap$org$apache$cassandra$auth$DataResource$Level[this.level.ordinal()]) {
      case 1:
         return "data";
      case 2:
         return String.format("%s/%s", new Object[]{"data", this.keyspace});
      case 3:
         return String.format("%s/%s/%s", new Object[]{"data", this.keyspace, this.table});
      default:
         throw new AssertionError();
      }
   }

   public IResource getParent() {
      switch(null.$SwitchMap$org$apache$cassandra$auth$DataResource$Level[this.level.ordinal()]) {
      case 2:
         return root();
      case 3:
         return keyspace(this.keyspace);
      default:
         throw new IllegalStateException("Root-level resource can't have a parent");
      }
   }

   public boolean isRootLevel() {
      return this.level == DataResource.Level.ROOT;
   }

   public boolean isKeyspaceLevel() {
      return this.level == DataResource.Level.KEYSPACE;
   }

   public boolean isTableLevel() {
      return this.level == DataResource.Level.TABLE;
   }

   public String getKeyspace() {
      if(this.isRootLevel()) {
         throw new IllegalStateException("ROOT data resource has no keyspace");
      } else {
         return this.keyspace;
      }
   }

   public String getTable() {
      if(!this.isTableLevel()) {
         throw new IllegalStateException(String.format("%s data resource has no table", new Object[]{this.level}));
      } else {
         return this.table;
      }
   }

   public boolean hasParent() {
      return this.level != DataResource.Level.ROOT;
   }

   public boolean exists() {
      switch(null.$SwitchMap$org$apache$cassandra$auth$DataResource$Level[this.level.ordinal()]) {
      case 1:
         return true;
      case 2:
         return Schema.instance.getKeyspaces().contains(this.keyspace);
      case 3:
         return Schema.instance.getTableMetadata(this.keyspace, this.table) != null;
      default:
         throw new AssertionError();
      }
   }

   public Set<Permission> applicablePermissions() {
      switch(null.$SwitchMap$org$apache$cassandra$auth$DataResource$Level[this.level.ordinal()]) {
      case 1:
      case 2:
         return KEYSPACE_LEVEL_PERMISSIONS;
      case 3:
         return TABLE_LEVEL_PERMISSIONS;
      default:
         throw new AssertionError();
      }
   }

   public IResource qualifyWithKeyspace(Supplier<String> keyspace) {
      return this.level == DataResource.Level.TABLE && this.keyspace == null?table((String)keyspace.get(), this.table):this;
   }

   public String toString() {
      switch(null.$SwitchMap$org$apache$cassandra$auth$DataResource$Level[this.level.ordinal()]) {
      case 1:
         return "<all keyspaces>";
      case 2:
         return String.format("<keyspace %s>", new Object[]{this.keyspace});
      case 3:
         return String.format("<table %s.%s>", new Object[]{this.keyspace, this.table});
      default:
         throw new AssertionError();
      }
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof DataResource)) {
         return false;
      } else {
         DataResource ds = (DataResource)o;
         return Objects.equals(this.level, ds.level) && Objects.equals(this.keyspace, ds.keyspace) && Objects.equals(this.table, ds.table);
      }
   }

   public int hashCode() {
      return this.hash;
   }

   static {
      TABLE_LEVEL_PERMISSIONS = Permissions.immutableSetOf(new Permission[]{CorePermission.ALTER, CorePermission.DROP, CorePermission.SELECT, CorePermission.MODIFY, CorePermission.AUTHORIZE});
      KEYSPACE_LEVEL_PERMISSIONS = Permissions.immutableSetOf(new Permission[]{CorePermission.CREATE, CorePermission.ALTER, CorePermission.DROP, CorePermission.SELECT, CorePermission.MODIFY, CorePermission.AUTHORIZE, CorePermission.DESCRIBE});
      ROOT_RESOURCE = new DataResource(DataResource.Level.ROOT, (String)null, (String)null);
   }

   static enum Level {
      ROOT,
      KEYSPACE,
      TABLE;

      private Level() {
      }
   }
}
