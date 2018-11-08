package com.datastax.bdp.system;

import com.datastax.bdp.cassandra.metrics.PerformanceObjectsPlugin;
import com.datastax.bdp.util.SchemaTool;
import com.google.common.collect.ImmutableMap;
import org.apache.cassandra.locator.EverywhereStrategy;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;

public class SolrKeyspace {
   public static final String NAME = "solr_admin";
   public static final String SOLR_FILTER_CACHE_STATS = "solr_filter_cache_stats";
   public static final String SOLR_INDEX_STATS = "solr_index_stats";
   public static final String SOLR_INDEXING_ERRORS = "solr_indexing_errors";
   public static final String SOLR_RESULT_CACHE_STATS = "solr_result_cache_stats";
   public static final String SOLR_SEARCH_REQUEST_HANDLER_METRICS = "solr_search_request_handler_metrics";
   public static final String SOLR_UPDATE_REQUEST_HANDLER_METRICS = "solr_update_request_handler_metrics";
   public static final String SOLR_SLOW_SUB_QUERY_LOG = "solr_slow_sub_query_log";
   public static final String SOLR_UPDATE_HANDLER_METRICS = "solr_update_handler_metrics";
   public static final String SOLR_COMMIT_LATENCY_SNAPHOT = "solr_commit_latency_snapshot";
   public static final String SOLR_MERGE_LATENCY_SNAPSHOT = "solr_merge_latency_snapshot";
   public static final String SOLR_QUERY_LATENCY_SNAPSHOT = "solr_query_latency_snapshot";
   public static final String SOLR_UPDATE_LATENCY_SNAPSHOT = "solr_update_latency_snapshot";
   public static final String SOLR_RESOURCES = "solr_resources";
   public static final String SOLR_RESOURCES_SCHEMA = "CREATE TABLE %s.%s (core_name text,resource_name text,resource_value blob,PRIMARY KEY(core_name, resource_name))";
   public static final String RESOURCE_NAME_COLUMN = "resource_name";
   public static final String RESOURCE_VALUE_COLUMN = "resource_value";
   private static final TableMetadata SolrResources = compile("solr_resources", "CREATE TABLE %s.%s (core_name text,resource_name text,resource_value blob,PRIMARY KEY(core_name, resource_name))");

   private SolrKeyspace() {
   }

   public static void maybeConfigure() {
      PerformanceObjectsPlugin.addUntracked(new String[]{"solr_admin"});
      SchemaTool.maybeCreateOrUpdateKeyspace(metadata());
   }

   private static TableMetadata compile(String tableName, String schema) {
      return DseSystemKeyspace.compile("solr_admin", tableName, schema);
   }

   public static TableMetadata maybeCreatePerformanceObjectTable(String tableName) {
      return PerformanceObjectsKeyspace.maybeCreateTable(tableName);
   }

   private static KeyspaceMetadata metadata() {
      return KeyspaceMetadata.create("solr_admin", KeyspaceParams.create(true, ImmutableMap.of("class", EverywhereStrategy.class.getCanonicalName())), tables());
   }

   private static Tables tables() {
      return Tables.of(new TableMetadata[]{SolrResources});
   }
}
