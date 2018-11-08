package com.datastax.bdp.config;

import com.datastax.bdp.cassandra.db.tiered.TieredStorageYamlConfig;
import com.datastax.bdp.db.audit.AuditLoggingOptions;
import java.util.HashMap;
import java.util.Map;

public class Config {
   public String delegated_snitch;
   public AuthenticationOptions authentication_options = new AuthenticationOptions();
   public RoleManagementOptions role_management_options = new RoleManagementOptions();
   public AuthorizationOptions authorization_options = new AuthorizationOptions();
   public KerberosOptions kerberos_options = new KerberosOptions();
   public LdapOptions ldap_options = new LdapOptions();
   public String system_key_directory;
   public String max_memory_to_lock_mb;
   public String max_memory_to_lock_fraction;
   public String back_pressure_threshold_per_core;
   public String flush_max_time_per_core;
   public boolean enable_index_disk_failure_policy;
   public String solr_data_dir;
   public boolean async_bootstrap_reindex;
   public String load_max_time_per_core;
   public boolean solr_field_cache_enabled;
   public String ram_buffer_heap_space_in_mb;
   public String ram_buffer_offheap_space_in_mb;
   public SolrIndexEncryptionOptions solr_encryption_options = new SolrIndexEncryptionOptions();
   public SolrTTLIndexRebuildOptions ttl_index_rebuild_options = new SolrTTLIndexRebuildOptions();
   public SolrShardTransportOptions shard_transport_options = new SolrShardTransportOptions();
   public String cql_solr_query_row_timeout;
   public String cql_solr_query_paging;
   public String lease_netty_server_port;
   public String performance_max_threads;
   public String performance_core_threads;
   public String performance_queue_capacity;
   public CqlSlowLogOptions cql_slow_log_options = new CqlSlowLogOptions();
   public AuditLoggingOptions audit_logging_options = new AuditLoggingOptions();
   public PerformanceObjectOptions.PeriodicallyUpdatedStatsOptions spark_cluster_info_options = new PerformanceObjectOptions.PeriodicallyUpdatedStatsOptions();
   public PerformanceObjectOptions.PeriodicallyUpdatedStatsOptions.SparkAppInfoOptions spark_application_info_options = new PerformanceObjectOptions.PeriodicallyUpdatedStatsOptions.SparkAppInfoOptions();
   public PerformanceObjectOptions.PeriodicallyUpdatedStatsOptions cql_system_info_options = new PerformanceObjectOptions.PeriodicallyUpdatedStatsOptions();
   public PerformanceObjectOptions.PeriodicallyUpdatedStatsOptions resource_level_latency_tracking_options = new PerformanceObjectOptions.PeriodicallyUpdatedStatsOptions();
   public PerformanceObjectOptions.UserLevelTrackingOptions user_level_latency_tracking_options = new PerformanceObjectOptions.UserLevelTrackingOptions();
   public PerformanceObjectOptions.PeriodicallyUpdatedStatsOptions db_summary_stats_options = new PerformanceObjectOptions.PeriodicallyUpdatedStatsOptions();
   public PerformanceObjectOptions.PeriodicallyUpdatedStatsOptions cluster_summary_stats_options = new PerformanceObjectOptions.PeriodicallyUpdatedStatsOptions();
   public PerformanceObjectOptions.PeriodicallyUpdatedStatsOptions lease_metrics_options = new PerformanceObjectOptions.PeriodicallyUpdatedStatsOptions();
   public PerformanceObjectOptions.HistogramDataTablesOptions histogram_data_options = new PerformanceObjectOptions.HistogramDataTablesOptions();
   public String initial_spark_worker_resources;
   public ResourceManagerOptions resource_manager_options = new ResourceManagerOptions();
   public String spark_daemon_readiness_assertion_interval;
   public String spark_shared_secret_bit_length;
   public boolean spark_security_enabled;
   public boolean spark_security_encryption_enabled;
   public SparkEncryptionOptions spark_encryption_options;
   public SystemTableEncryptionOptions system_info_encryption = new SystemTableEncryptionOptions();
   public boolean config_encryption_active = false;
   public String config_encryption_key_name = "system_key";
   public PerformanceObjectOptions.ThresholdPerformanceObjectOptions solr_slow_sub_query_log_options = new PerformanceObjectOptions.ThresholdPerformanceObjectOptions();
   public PerformanceObjectOptions.PeriodicallyUpdatedStatsOptions solr_update_handler_metrics_options = new PerformanceObjectOptions.PeriodicallyUpdatedStatsOptions();
   public PerformanceObjectOptions.PeriodicallyUpdatedStatsOptions solr_request_handler_metrics_options = new PerformanceObjectOptions.PeriodicallyUpdatedStatsOptions();
   public PerformanceObjectOptions.PeriodicallyUpdatedStatsOptions solr_index_stats_options = new PerformanceObjectOptions.PeriodicallyUpdatedStatsOptions();
   public PerformanceObjectOptions.PeriodicallyUpdatedStatsOptions solr_cache_stats_options = new PerformanceObjectOptions.PeriodicallyUpdatedStatsOptions();
   public PerformanceObjectOptions.PeriodicallyUpdatedStatsOptions solr_latency_snapshot_options = new PerformanceObjectOptions.PeriodicallyUpdatedStatsOptions();
   public NodeHealthOptions node_health_options = new NodeHealthOptions();
   public boolean enable_health_based_routing;
   public Map<String, KmipHostOptions> kmip_hosts = new HashMap();
   public Map<String, TieredStorageYamlConfig> tiered_storage_options = new HashMap();
   public GraphEventOptions graph_events;
   public AdvancedReplicationOptions advanced_replication_options = new AdvancedReplicationOptions();
   public String server_id = "";
   public DseFsOptions dsefs_options = new DseFsOptions();
   public AlwaysOnSqlOptions alwayson_sql_options = new AlwaysOnSqlOptions();
   public InternodeMessagingOptions internode_messaging_options = new InternodeMessagingOptions();
   public String solr_resource_upload_limit_mb;
   public SparkUIOptions spark_ui_options = new SparkUIOptions();
   public SparkProcessRunnerOptions spark_process_runner = new SparkProcessRunnerOptions();

   public Config() {
   }
}
