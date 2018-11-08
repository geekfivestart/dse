package com.datastax.bdp.system;

import com.datastax.bdp.util.SchemaTool;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;

public class DseSecurityKeyspace {
   public static final String NAME = "dse_security";
   public static final String DIGEST_TOKENS = "digest_tokens";
   public static final String ROLE_OPTIONS = "role_options";
   public static final String SPARK_SECURITY = "spark_security";
   private static final TableMetadata DigestTokens = compile("digest_tokens", "digest_tokens", "CREATE TABLE %s (id blob,password blob,PRIMARY KEY(id))");
   private static final TableMetadata RoleOptions = compile("role_options", "role options", "CREATE TABLE %s (role text,options map<text, text>,PRIMARY KEY(role))");
   private static final TableMetadata SparkSecurity = compile("spark_security", "Spark security settings", "CREATE TABLE %s (dc TEXT PRIMARY KEY, shared_secret TEXT)");

   public DseSecurityKeyspace() {
   }

   private static TableMetadata compile(String name, String description, String schema) {
      return CreateTableStatement.parse(String.format(schema, new Object[]{name}), "dse_security").id(SchemaTool.tableIdForDseSystemTable("dse_security", name)).comment(description).gcGraceSeconds((int)TimeUnit.DAYS.toSeconds(90L)).build();
   }

   public static KeyspaceMetadata metadata() {
      return KeyspaceMetadata.create("dse_security", KeyspaceParams.simple(1), tables());
   }

   private static Tables tables() {
      return Tables.of(new TableMetadata[]{DigestTokens, RoleOptions, SparkSecurity});
   }

   public static void maybeConfigureKeyspace() {
      SchemaTool.maybeCreateOrUpdateKeyspace(metadata(), 0L);
   }

   public static enum SparkSecurity {
      SHARED_SECRET("shared_secret");

      public final String columnName;

      private SparkSecurity(String columnName) {
         this.columnName = columnName;
      }

      public String toString() {
         return this.columnName;
      }
   }
}
