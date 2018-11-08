package org.apache.cassandra.auth;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;

public final class AuthKeyspace {
   public static final String ROLES = "roles";
   public static final String ROLE_MEMBERS = "role_members";
   public static final String ROLE_PERMISSIONS = "role_permissions";
   private static final TableMetadata Roles = parse("roles", "role definitions", "CREATE TABLE %s (role text,is_superuser boolean,can_login boolean,salted_hash text,member_of set<text>,PRIMARY KEY(role))");
   private static final TableMetadata RoleMembers = parse("role_members", "role memberships lookup table", "CREATE TABLE %s (role text,member text,PRIMARY KEY(role, member))");
   private static final TableMetadata RolePermissions = parse("role_permissions", "permissions granted to db roles", "CREATE TABLE %s (role text,resource text,permissions set<text>,restricted set<text>,grantables set<text>,PRIMARY KEY(role, resource))");
   public static final KeyspaceMetadata metadata;

   private AuthKeyspace() {
   }

   private static TableMetadata parse(String name, String description, String cql) {
      return CreateTableStatement.parse(String.format(cql, new Object[]{name}), "system_auth").id(TableId.forSystemTable("system_auth", name)).comment(description).dcLocalReadRepairChance(0.0D).gcGraceSeconds((int)TimeUnit.DAYS.toSeconds(90L)).build();
   }

   public static KeyspaceMetadata metadata() {
      return metadata;
   }

   public static List<TableMetadata> tablesIfNotExist() {
      return Collections.singletonList(RolePermissions);
   }

   static {
      metadata = KeyspaceMetadata.create("system_auth", KeyspaceParams.simple(1), Tables.of(new TableMetadata[]{Roles, RoleMembers}));
   }
}
