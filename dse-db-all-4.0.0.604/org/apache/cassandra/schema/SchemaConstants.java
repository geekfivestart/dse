package org.apache.cassandra.schema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.cassandra.utils.HashingUtils;

public final class SchemaConstants {
   public static final Pattern PATTERN_WORD_CHARS = Pattern.compile("\\w+");
   public static final String SYSTEM_KEYSPACE_NAME = "system";
   public static final String SCHEMA_KEYSPACE_NAME = "system_schema";
   public static final String TRACE_KEYSPACE_NAME = "system_traces";
   public static final String AUTH_KEYSPACE_NAME = "system_auth";
   public static final String DISTRIBUTED_KEYSPACE_NAME = "system_distributed";
   public static final String DSE_INTERNAL_KEYSPACE_PREFIX = "dse_";
   public static final Set<String> LOCAL_SYSTEM_KEYSPACE_NAMES = ImmutableSet.of("system", "system_schema");
   public static final Set<String> REPLICATED_SYSTEM_KEYSPACE_NAMES = ImmutableSet.of("system_traces", "system_auth", "system_distributed");
   public static final int NAME_LENGTH = 48;
   public static final UUID emptyVersion;
   public static final List<String> LEGACY_AUTH_TABLES = ImmutableList.of("credentials", "users", "permissions");
   public static final List<String> OBSOLETE_AUTH_TABLES = ImmutableList.of("resource_role_permissons_index");

   public SchemaConstants() {
   }

   public static boolean isValidName(String name) {
      return name != null && !name.isEmpty() && name.length() <= 48 && PATTERN_WORD_CHARS.matcher(name).matches();
   }

   public static boolean isLocalSystemKeyspace(String keyspaceName) {
      return LOCAL_SYSTEM_KEYSPACE_NAMES.contains(keyspaceName.toLowerCase());
   }

   public static boolean isReplicatedSystemKeyspace(String keyspaceName) {
      return REPLICATED_SYSTEM_KEYSPACE_NAMES.contains(keyspaceName.toLowerCase());
   }

   public static boolean isUserKeyspace(String keyspaceName) {
      return !isLocalSystemKeyspace(keyspaceName) && !isReplicatedSystemKeyspace(keyspaceName);
   }

   public static boolean isInternalKeyspace(String keyspaceName) {
      return isLocalSystemKeyspace(keyspaceName) || isReplicatedSystemKeyspace(keyspaceName) || keyspaceName.toLowerCase().startsWith("dse_");
   }

   public static boolean isSchemaKeyspace(String keyspaceName) {
      return keyspaceName.toLowerCase().equals("system_schema");
   }

   static {
      emptyVersion = UUID.nameUUIDFromBytes(HashingUtils.CURRENT_HASH_FUNCTION.newHasher().hash().asBytes());
   }
}
