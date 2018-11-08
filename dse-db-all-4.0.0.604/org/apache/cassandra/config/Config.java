package org.apache.cassandra.config;

import com.datastax.bdp.db.audit.AuditLoggingOptions;
import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.db.ConsistencyLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Config {
   private static final Logger logger = LoggerFactory.getLogger(Config.class);
   public static final String PROPERTY_PREFIX = "cassandra.";
   public String cluster_name = "Test Cluster";
   public String authenticator;
   public String authorizer;
   public String role_manager;
   public volatile int permissions_validity_in_ms = 120000;
   public volatile int permissions_cache_max_entries = 1000;
   public volatile int permissions_cache_initial_capacity = 128;
   public volatile int permissions_update_interval_in_ms = -1;
   public volatile int roles_validity_in_ms = 120000;
   public volatile int roles_cache_max_entries = 1000;
   public volatile int roles_cache_initial_capacity = 128;
   public volatile int roles_update_interval_in_ms = -1;
   public boolean system_keyspaces_filtering;
   public String system_key_directory;
   public SystemTableEncryptionOptions system_info_encryption = new SystemTableEncryptionOptions();
   public Integer tpc_cores;
   public Integer tpc_io_cores;
   public Integer io_global_queue_depth;
   public String partitioner;
   public boolean auto_bootstrap = true;
   public volatile boolean hinted_handoff_enabled = true;
   public Set<String> hinted_handoff_disabled_datacenters = Sets.newConcurrentHashSet();
   public volatile int max_hint_window_in_ms = 10800000;
   public String hints_directory;
   public ParameterizedClass seed_provider;
   public Config.DiskAccessMode disk_access_mode;
   public Config.DiskFailurePolicy disk_failure_policy;
   public Config.CommitFailurePolicy commit_failure_policy;
   public String initial_token;
   public int num_tokens;
   public String allocate_tokens_for_keyspace;
   public Integer allocate_tokens_for_local_replication_factor;
   public volatile long request_timeout_in_ms;
   public volatile long read_request_timeout_in_ms;
   public volatile long range_request_timeout_in_ms;
   public volatile long aggregated_request_timeout_in_ms;
   public volatile long write_request_timeout_in_ms;
   public volatile long cross_dc_rtt_in_ms;
   public volatile long counter_write_request_timeout_in_ms;
   public volatile long cas_contention_timeout_in_ms;
   public volatile long truncate_request_timeout_in_ms;
   public Integer streaming_connections_per_host;
   public Integer streaming_keep_alive_period_in_secs;
   public boolean cross_node_timeout;
   public volatile long slow_query_log_timeout_in_ms;
   public volatile double phi_convict_threshold;
   public int memtable_flush_writers;
   public Integer memtable_heap_space_in_mb;
   public Integer memtable_offheap_space_in_mb;
   public Double memtable_cleanup_threshold;
   public int storage_port;
   public int ssl_storage_port;
   public String listen_address;
   public String listen_interface;
   public boolean listen_interface_prefer_ipv6;
   public String broadcast_address;
   public boolean listen_on_broadcast_address;
   public String internode_authenticator;
   public String native_transport_address;
   public String native_transport_interface;
   public Boolean native_transport_interface_prefer_ipv6;
   public String native_transport_broadcast_address;
   public Boolean native_transport_keepalive;
   public int internode_send_buff_size_in_bytes;
   public int internode_recv_buff_size_in_bytes;
   public boolean start_native_transport;
   public int native_transport_port;
   public Integer native_transport_port_ssl;
   public int native_transport_max_threads;
   public int native_transport_max_frame_size_in_mb;
   public volatile long native_transport_max_concurrent_connections;
   public volatile long native_transport_max_concurrent_connections_per_ip;
   public int max_value_size_in_mb;
   public boolean snapshot_before_compaction;
   public boolean auto_snapshot;
   public int column_index_size_in_kb;
   public int column_index_cache_size_in_kb;
   public volatile int batch_size_warn_threshold_in_kb;
   public volatile int batch_size_fail_threshold_in_kb;
   public Integer unlogged_batch_across_partitions_warn_threshold;
   public volatile Integer concurrent_compactors;
   public volatile int compaction_throughput_mb_per_sec;
   public volatile int compaction_large_partition_warning_threshold_mb;
   public int min_free_space_per_drive_in_mb;
   public volatile int concurrent_validations;
   public volatile int concurrent_materialized_view_builders;
   public volatile int tpc_concurrent_requests_limit;
   public volatile int tpc_pending_requests_limit;
   public volatile int stream_throughput_outbound_megabits_per_sec;
   public volatile int inter_dc_stream_throughput_outbound_megabits_per_sec;
   public String[] data_file_directories;
   public String saved_caches_directory;
   public String commitlog_directory;
   public Integer commitlog_total_space_in_mb;
   public Config.CommitLogSync commitlog_sync;
   /** @deprecated */
   public double commitlog_sync_batch_window_in_ms;
   public double commitlog_sync_group_window_in_ms;
   public int commitlog_sync_period_in_ms;
   public int commitlog_segment_size_in_mb;
   public ParameterizedClass commitlog_compression;
   public int commitlog_max_compression_buffers_in_pool;
   public TransparentDataEncryptionOptions transparent_data_encryption_options;
   public Integer max_mutation_size_in_kb;
   public boolean cdc_enabled;
   public String cdc_raw_directory;
   public int cdc_total_space_in_mb;
   public int cdc_free_space_check_interval_ms;
   /** @deprecated */
   @Deprecated
   public int commitlog_periodic_queue_size;
   public String endpoint_snitch;
   public boolean dynamic_snitch;
   public int dynamic_snitch_update_interval_in_ms;
   public int dynamic_snitch_reset_interval_in_ms;
   public double dynamic_snitch_badness_threshold;
   public EncryptionOptions.ServerEncryptionOptions server_encryption_options;
   public EncryptionOptions.ClientEncryptionOptions client_encryption_options;
   public EncryptionOptions.ServerEncryptionOptions encryption_options;
   public Config.InternodeCompression internode_compression;
   public int hinted_handoff_throttle_in_kb;
   public int batchlog_replay_throttle_in_kb;
   public Config.BatchlogEndpointStrategy batchlog_endpoint_strategy;
   public int max_hints_delivery_threads;
   public int hints_flush_period_in_ms;
   public int max_hints_file_size_in_mb;
   public ParameterizedClass hints_compression;
   public int sstable_preemptive_open_interval_in_mb;
   public volatile boolean incremental_backups;
   public boolean trickle_fsync;
   public int trickle_fsync_interval_in_kb;
   public Long key_cache_size_in_mb;
   public volatile int key_cache_save_period;
   public volatile int key_cache_keys_to_save;
   public String row_cache_class_name;
   public long row_cache_size_in_mb;
   public volatile int row_cache_save_period;
   public volatile int row_cache_keys_to_save;
   public Long counter_cache_size_in_mb;
   public volatile int counter_cache_save_period;
   public volatile int counter_cache_keys_to_save;
   private static boolean isClientMode = false;
   public Integer file_cache_size_in_mb;
   public Boolean file_cache_round_up;
   public boolean buffer_pool_use_heap_if_exhausted;
   public Config.DiskOptimizationStrategy disk_optimization_strategy;
   public double disk_optimization_estimate_percentile;
   public double disk_optimization_page_cross_chance;
   public boolean inter_dc_tcp_nodelay;
   public Config.MemtableAllocationType memtable_allocation_type;
   public volatile int tombstone_warn_threshold;
   public volatile int tombstone_failure_threshold;
   /** @deprecated */
   @Deprecated
   public volatile Long index_summary_capacity_in_mb;
   /** @deprecated */
   @Deprecated
   public volatile int index_summary_resize_interval_in_minutes;
   public int gc_log_threshold_in_ms;
   public int gc_warn_threshold_in_ms;
   public volatile double seed_gossip_probability;
   public int tracetype_query_ttl;
   public int tracetype_repair_ttl;
   public volatile ConsistencyLevel ideal_consistency_level;
   public String otc_coalescing_strategy;
   public static final int otc_coalescing_window_us_default = 200;
   public int otc_coalescing_window_us;
   public int otc_coalescing_enough_coalesced_messages;
   /** @deprecated */
   @Deprecated
   public volatile int otc_backlog_expiration_interval_ms;
   public int windows_timer_interval;
   public Long prepared_statements_cache_size_mb;
   public boolean enable_user_defined_functions;
   public boolean enable_scripted_user_defined_functions;
   public boolean enable_user_defined_functions_threads;
   public long user_defined_function_warn_timeout;
   public long user_defined_function_fail_timeout;
   public Config.UserFunctionTimeoutPolicy user_function_timeout_policy;
   public volatile boolean back_pressure_enabled;
   public volatile ParameterizedClass back_pressure_strategy;
   public boolean pick_level_on_streaming;
   public ContinuousPagingConfig continuous_paging;
   public int max_memory_to_lock_mb;
   public double max_memory_to_lock_fraction;
   public int metrics_histogram_update_interval_millis;
   public AuditLoggingOptions audit_logging_options;
   public NodeSyncConfig nodesync;
   public Config.RepairCommandPoolFullStrategy repair_command_pool_full_strategy;
   public int repair_command_pool_size;
   private static final List<String> SENSITIVE_KEYS = new ArrayList<String>() {
      {
         this.add("client_encryption_options");
         this.add("server_encryption_options");
      }
   };
   /** @deprecated */
   @Deprecated
   public String rpc_address;
   /** @deprecated */
   @Deprecated
   public String rpc_interface;
   /** @deprecated */
   @Deprecated
   public Boolean rpc_interface_prefer_ipv6;
   /** @deprecated */
   @Deprecated
   public String broadcast_rpc_address;
   /** @deprecated */
   @Deprecated
   public Boolean rpc_keepalive;

   public Config() {
      this.disk_access_mode = Config.DiskAccessMode.auto;
      this.disk_failure_policy = Config.DiskFailurePolicy.ignore;
      this.commit_failure_policy = Config.CommitFailurePolicy.stop;
      this.num_tokens = 1;
      this.allocate_tokens_for_keyspace = null;
      this.allocate_tokens_for_local_replication_factor = null;
      this.request_timeout_in_ms = 10000L;
      this.read_request_timeout_in_ms = 5000L;
      this.range_request_timeout_in_ms = 10000L;
      this.aggregated_request_timeout_in_ms = 120000L;
      this.write_request_timeout_in_ms = 2000L;
      this.cross_dc_rtt_in_ms = 0L;
      this.counter_write_request_timeout_in_ms = 5000L;
      this.cas_contention_timeout_in_ms = 1000L;
      this.truncate_request_timeout_in_ms = 60000L;
      this.streaming_connections_per_host = Integer.valueOf(1);
      this.streaming_keep_alive_period_in_secs = Integer.valueOf(300);
      this.cross_node_timeout = false;
      this.slow_query_log_timeout_in_ms = 500L;
      this.phi_convict_threshold = 8.0D;
      this.memtable_flush_writers = 0;
      this.memtable_cleanup_threshold = null;
      this.storage_port = 7000;
      this.ssl_storage_port = 7001;
      this.listen_interface_prefer_ipv6 = false;
      this.listen_on_broadcast_address = false;
      this.internode_send_buff_size_in_bytes = 0;
      this.internode_recv_buff_size_in_bytes = 0;
      this.start_native_transport = true;
      this.native_transport_port = 9042;
      this.native_transport_port_ssl = null;
      this.native_transport_max_threads = 128;
      this.native_transport_max_frame_size_in_mb = 256;
      this.native_transport_max_concurrent_connections = -1L;
      this.native_transport_max_concurrent_connections_per_ip = -1L;
      this.max_value_size_in_mb = 256;
      this.snapshot_before_compaction = false;
      this.auto_snapshot = true;
      this.column_index_size_in_kb = 64;
      this.column_index_cache_size_in_kb = 2;
      this.batch_size_warn_threshold_in_kb = 5;
      this.batch_size_fail_threshold_in_kb = 50;
      this.unlogged_batch_across_partitions_warn_threshold = Integer.valueOf(10);
      this.compaction_throughput_mb_per_sec = 16;
      this.compaction_large_partition_warning_threshold_mb = 100;
      this.min_free_space_per_drive_in_mb = 50;
      this.concurrent_validations = 2147483647;
      this.concurrent_materialized_view_builders = 2;
      this.tpc_concurrent_requests_limit = 128;
      this.tpc_pending_requests_limit = 256;
      this.stream_throughput_outbound_megabits_per_sec = 200;
      this.inter_dc_stream_throughput_outbound_megabits_per_sec = 200;
      this.data_file_directories = new String[0];
      this.commitlog_sync_batch_window_in_ms = 0.0D / 0.0;
      this.commitlog_sync_group_window_in_ms = 0.0D / 0.0;
      this.commitlog_segment_size_in_mb = 32;
      this.commitlog_max_compression_buffers_in_pool = 3;
      this.transparent_data_encryption_options = new TransparentDataEncryptionOptions();
      this.cdc_enabled = false;
      this.cdc_total_space_in_mb = 0;
      this.cdc_free_space_check_interval_ms = 250;
      this.commitlog_periodic_queue_size = -1;
      this.dynamic_snitch = true;
      this.dynamic_snitch_update_interval_in_ms = 500;
      this.dynamic_snitch_reset_interval_in_ms = 600000;
      this.dynamic_snitch_badness_threshold = 0.1D;
      this.server_encryption_options = new EncryptionOptions.ServerEncryptionOptions();
      this.client_encryption_options = new EncryptionOptions.ClientEncryptionOptions();
      this.internode_compression = Config.InternodeCompression.none;
      this.hinted_handoff_throttle_in_kb = 1024;
      this.batchlog_replay_throttle_in_kb = 1024;
      this.batchlog_endpoint_strategy = Config.BatchlogEndpointStrategy.random_remote;
      this.max_hints_delivery_threads = 2;
      this.hints_flush_period_in_ms = 10000;
      this.max_hints_file_size_in_mb = 128;
      this.sstable_preemptive_open_interval_in_mb = 50;
      this.incremental_backups = false;
      this.trickle_fsync = false;
      this.trickle_fsync_interval_in_kb = 10240;
      this.key_cache_size_in_mb = null;
      this.key_cache_save_period = 14400;
      this.key_cache_keys_to_save = 2147483647;
      this.row_cache_class_name = "org.apache.cassandra.cache.OHCProvider";
      this.row_cache_size_in_mb = 0L;
      this.row_cache_save_period = 0;
      this.row_cache_keys_to_save = 2147483647;
      this.counter_cache_size_in_mb = null;
      this.counter_cache_save_period = 7200;
      this.counter_cache_keys_to_save = 2147483647;
      this.buffer_pool_use_heap_if_exhausted = false;
      this.disk_optimization_strategy = Config.DiskOptimizationStrategy.ssd;
      this.disk_optimization_estimate_percentile = 0.95D;
      this.disk_optimization_page_cross_chance = 0.1D;
      this.inter_dc_tcp_nodelay = true;
      this.memtable_allocation_type = Config.MemtableAllocationType.heap_buffers;
      this.tombstone_warn_threshold = 1000;
      this.tombstone_failure_threshold = 100000;
      this.index_summary_resize_interval_in_minutes = 60;
      this.gc_log_threshold_in_ms = 200;
      this.gc_warn_threshold_in_ms = 1000;
      this.seed_gossip_probability = 1.0D;
      this.tracetype_query_ttl = (int)TimeUnit.DAYS.toSeconds(1L);
      this.tracetype_repair_ttl = (int)TimeUnit.DAYS.toSeconds(7L);
      this.ideal_consistency_level = null;
      this.otc_coalescing_strategy = "DISABLED";
      this.otc_coalescing_window_us = 200;
      this.otc_coalescing_enough_coalesced_messages = 8;
      this.otc_backlog_expiration_interval_ms = -1;
      this.windows_timer_interval = 0;
      this.prepared_statements_cache_size_mb = null;
      this.enable_user_defined_functions = false;
      this.enable_scripted_user_defined_functions = false;
      this.enable_user_defined_functions_threads = true;
      this.user_defined_function_warn_timeout = 500L;
      this.user_defined_function_fail_timeout = 1500L;
      this.user_function_timeout_policy = Config.UserFunctionTimeoutPolicy.die;
      this.back_pressure_enabled = false;
      this.continuous_paging = new ContinuousPagingConfig();
      this.max_memory_to_lock_mb = 0;
      this.max_memory_to_lock_fraction = 0.2D;
      this.metrics_histogram_update_interval_millis = 1000;
      this.audit_logging_options = new AuditLoggingOptions();
      this.nodesync = new NodeSyncConfig();
      this.repair_command_pool_full_strategy = Config.RepairCommandPoolFullStrategy.queue;
      this.repair_command_pool_size = this.concurrent_validations;
      this.rpc_address = null;
      this.rpc_interface = null;
      this.rpc_interface_prefer_ipv6 = null;
      this.broadcast_rpc_address = null;
      this.rpc_keepalive = null;
   }

   /** @deprecated */
   @Deprecated
   public static boolean isClientMode() {
      return isClientMode;
   }

   /** @deprecated */
   @Deprecated
   public static void setClientMode(boolean clientMode) {
      isClientMode = clientMode;
   }

   public ConsistencyLevel getAuditCassConsistencyLevel() {
      return ConsistencyLevel.valueOf(this.audit_logging_options.cassandra_audit_writer_options.write_consistency);
   }

   public String getAuditLoggerCassMode() {
      return this.audit_logging_options.cassandra_audit_writer_options.mode;
   }

   public int getAuditCassFlushTime() {
      return Integer.parseInt(this.audit_logging_options.cassandra_audit_writer_options.flush_time);
   }

   public int getAuditCassBatchSize() {
      return Integer.parseInt(this.audit_logging_options.cassandra_audit_writer_options.batch_size);
   }

   public int getAuditLoggerCassAsyncQueueSize() {
      return Integer.parseInt(this.audit_logging_options.cassandra_audit_writer_options.queue_size);
   }

   public static void log(Config config) {
      Map<String, String> configMap = new TreeMap();
      Field[] var2 = Config.class.getFields();
      int var3 = var2.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         Field field = var2[var4];
         if(!Modifier.isFinal(field.getModifiers())) {
            String name = field.getName();
            if(SENSITIVE_KEYS.contains(name)) {
               configMap.put(name, "<REDACTED>");
            } else {
               String value;
               try {
                  value = field.get(config).toString();
               } catch (IllegalAccessException | NullPointerException var9) {
                  value = "null";
               }

               configMap.put(name, value);
            }
         }
      }

      logger.info("Node configuration:[{}]", Joiner.on("; ").join(configMap.entrySet()));
   }

   public static enum BatchlogEndpointStrategy {
      random_remote(false, false),
      dynamic_remote(true, false),
      dynamic(true, true);

      public final boolean dynamicSnitch;
      public final boolean allowLocalRack;

      private BatchlogEndpointStrategy(boolean dynamicSnitch, boolean allowLocalRack) {
         this.dynamicSnitch = dynamicSnitch;
         this.allowLocalRack = allowLocalRack;
      }
   }

   public static enum RepairCommandPoolFullStrategy {
      queue,
      reject;

      private RepairCommandPoolFullStrategy() {
      }
   }

   public static enum DiskOptimizationStrategy {
      ssd,
      spinning;

      private DiskOptimizationStrategy() {
      }
   }

   public static enum UserFunctionTimeoutPolicy {
      ignore,
      die,
      die_immediate;

      private UserFunctionTimeoutPolicy() {
      }
   }

   public static enum CommitFailurePolicy {
      stop,
      stop_commit,
      ignore,
      die;

      private CommitFailurePolicy() {
      }
   }

   public static enum DiskFailurePolicy {
      best_effort,
      stop,
      ignore,
      stop_paranoid,
      die;

      private DiskFailurePolicy() {
      }
   }

   public static enum MemtableAllocationType {
      unslabbed_heap_buffers,
      heap_buffers,
      offheap_buffers,
      offheap_objects;

      private MemtableAllocationType() {
      }
   }

   public static enum DiskAccessMode {
      auto(Config.AccessMode.standard, Config.AccessMode.standard, Config.AccessMode.standard),
      mmap(Config.AccessMode.mmap, Config.AccessMode.mmap, Config.AccessMode.mmap),
      mmap_reads(Config.AccessMode.mmap, Config.AccessMode.mmap, Config.AccessMode.standard),
      mmap_index_only(Config.AccessMode.standard, Config.AccessMode.mmap, Config.AccessMode.standard),
      mmap_commitlog_only(Config.AccessMode.standard, Config.AccessMode.standard, Config.AccessMode.mmap),
      standard(Config.AccessMode.standard, Config.AccessMode.standard, Config.AccessMode.standard);

      final Config.AccessMode data;
      final Config.AccessMode index;
      final Config.AccessMode commitlog;

      private DiskAccessMode(Config.AccessMode data, Config.AccessMode index, Config.AccessMode commitlog) {
         this.data = data;
         this.index = index;
         this.commitlog = commitlog;
      }
   }

   public static enum AccessMode {
      standard,
      mmap;

      private AccessMode() {
      }
   }

   public static enum InternodeCompression {
      all,
      none,
      dc;

      private InternodeCompression() {
      }
   }

   public static enum CommitLogSync {
      periodic,
      batch,
      group;

      private CommitLogSync() {
      }
   }
}
