package com.datastax.bdp.system;

import com.datastax.bdp.cassandra.metrics.PerformanceObjectsPlugin;
import com.datastax.bdp.util.SchemaTool;
import com.google.common.collect.ImmutableMap;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;

public final class DseLocalKeyspace {
   public static final String NAME = "dse_system_local";
   private static final TableMetadata SolrLocalResources = DseSystemKeyspace.compile("dse_system_local", "solr_resources", "CREATE TABLE %s.%s (core_name text,resource_name text,resource_value blob,PRIMARY KEY(core_name, resource_name))");

   private DseLocalKeyspace() {
   }

   public static void maybeConfigure() {
      PerformanceObjectsPlugin.addUntracked(new String[]{"dse_system_local"});
      SchemaTool.maybeCreateOrUpdateKeyspace(metadata());
   }

   private static KeyspaceMetadata metadata() {
      ImmutableMap<String, String> replication = ImmutableMap.of("class", LocalStrategy.class.getCanonicalName());
      return KeyspaceMetadata.create("dse_system_local", KeyspaceParams.create(true, replication), tables());
   }

   private static Tables tables() {
      return Tables.of(new TableMetadata[]{SolrLocalResources});
   }
}
