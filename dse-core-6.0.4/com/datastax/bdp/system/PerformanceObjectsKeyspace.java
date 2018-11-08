package com.datastax.bdp.system;

import com.datastax.bdp.cassandra.metrics.PerformanceObjectsPlugin;
import com.datastax.bdp.util.SchemaTool;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.UnmodifiableIterator;
import com.google.inject.Inject;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerformanceObjectsKeyspace {
   public static final String NAME = "dse_perf";
   public static final String CELL_COUNT_HISTOGRAMS = "cell_count_histograms";
   public static final String CLUSTER_SNAPSHOT = "cluster_snapshot";
   public static final String DC_SNAPSHOT = "dc_snapshot";
   public static final String DROPPED_MESSAGES = "dropped_messages";
   public static final String KEY_CACHE = "key_cache";
   public static final String KEYSPACE_SNAPSHOT = "keyspace_snapshot";
   public static final String NET_STATS = "net_stats";
   public static final String NODE_SLOW_LOG = "node_slow_log";
   public static final String NODE_SNAPSHOT = "node_snapshot";
   public static final String NODE_TABLE_SNAPSHOT = "node_table_snapshot";
   public static final String OBJECT_IO = "object_io";
   public static final String OBJECT_READ_IO_SNAPSHOT = "object_read_io_snapshot";
   public static final String OBJECT_USER_IO = "object_user_io";
   public static final String OBJECT_USER_READ_IO_SNAPSHOT = "object_user_read_io_snapshot";
   public static final String OBJECT_USER_WRITE_IO_SNAPSHOT = "object_user_write_io_snapshot";
   public static final String OBJECT_WRITE_IO_SNAPSHOT = "object_write_io_snapshot";
   public static final String PARTITION_SIZE_HISTOGRAMS = "partition_size_histograms";
   public static final String RANGE_LATENCY_HISTOGRAMS = "range_latency_histograms";
   public static final String READ_LATENCY_HISTOGRAMS = "read_latency_histograms";
   public static final String SSTABLES_PER_READ_HISTOGRAMS = "sstables_per_read_histograms";
   public static final String TABLE_SNAPSHOT = "table_snapshot";
   public static final String THREAD_POOL = "thread_pool";
   public static final String THREAD_POOL_MESSAGES = "thread_pool_messages";
   public static final String USER_IO = "user_io";
   public static final String USER_OBJECT_IO = "user_object_io";
   public static final String USER_OBJECT_READ_IO_SNAPSHOT = "user_object_read_io_snapshot";
   public static final String USER_OBJECT_WRITE_IO_SNAPSHOT = "user_object_write_io_snapshot";
   public static final String USER_READ_IO_SNAPSHOT = "user_read_io_snapshot";
   public static final String USER_WRITE_IO_SNAPSHOT = "user_write_io_snapshot";
   public static final String WRITE_LATENCY_HISTOGRAMS = "write_latency_histograms";
   public static final List<String> GLOBAL_METRICS = ImmutableList.builder().add(new String[]{"range_latency_histograms", "read_latency_histograms", "write_latency_histograms"}).build();
   public static final List<String> KEYSPACE_METRICS;
   public static final ImmutableList<String> TABLE_METRICS;
   public static final String LEASES = "leases";
   public static final String NODE_SLOW_LOG_TRACING_SESSION_ID = "tracing_session_id";
   public static final String SCHEMA_ADD_NODE_SLOW_LOG_TRACING_SESSION_ID;
   public static final String SNAPSHOT_BACKGROUND_IO_PENDING = "background_io_pending";
   private static final String ADD_BIGINT_COLUMN = "ALTER TABLE %s.%s ADD %s bigint;";
   public static final String NODE_SNAPSHOT_ADD_BACKGROUND_IO_PENDING;
   public static final String CLUSTER_SNAPSHOT_ADD_BACKGROUND_IO_PENDING;
   public static final String DC_SNAPSHOT_ADD_BACKGROUND_IO_PENDING;
   private static final Map<String, TableMetadata> tables;
   private static final CountDownLatch initialized;
   private static final Logger logger;
   @Inject
   public static Set<PerformanceObjectsKeyspace.TableDef> extraTables;

   private PerformanceObjectsKeyspace() {
   }

   public static synchronized void init() {
      if(initialized.getCount() != 0L) {
         String histogramsTemplate = "CREATE TABLE %s.%s (node_ip inet, keyspace_name text, table_name text, histogram_id timestamp, bucket_offset bigint, bucket_count bigint, PRIMARY KEY ((node_ip, keyspace_name, table_name), histogram_id, bucket_offset)) WITH CLUSTERING ORDER BY (histogram_id DESC, bucket_offset ASC) AND " + PerformanceObjectsPlugin.getAdditionalTableOptions();
         String histogramSummary = "CREATE TABLE IF NOT EXISTS %s.%s (node_ip inet, keyspace_name text, table_name text, histogram_id timestamp, p50 bigint, p75 bigint, p90 bigint, p95 bigint, p98 bigint, p99 bigint, min bigint, max bigint, dropped_mutations bigint, PRIMARY KEY ((node_ip, keyspace_name, table_name), histogram_id)) WITH CLUSTERING ORDER BY (histogram_id DESC) AND" + PerformanceObjectsPlugin.getAdditionalTableOptions();
         UnmodifiableIterator var2 = TABLE_METRICS.iterator();

         String globalMetrics;
         while(var2.hasNext()) {
            globalMetrics = (String)var2.next();
            compile(globalMetrics, histogramsTemplate);
            compile(globalMetrics + "_summary", histogramSummary);
         }

         String keyspaceMetrics = "CREATE TABLE IF NOT EXISTS %s.%s (node_ip inet, keyspace_name text, histogram_id timestamp, bucket_offset bigint, bucket_count bigint, PRIMARY KEY ((node_ip, keyspace_name), histogram_id, bucket_offset)) WITH CLUSTERING ORDER BY (histogram_id DESC, bucket_offset ASC) AND " + PerformanceObjectsPlugin.getAdditionalTableOptions();
         Iterator var7 = KEYSPACE_METRICS.iterator();

         while(var7.hasNext()) {
            String tableName = (String)var7.next();
            compile(tableName + "_ks", keyspaceMetrics);
         }

         globalMetrics = "CREATE TABLE IF NOT EXISTS %s.%s (node_ip inet, histogram_id timestamp, bucket_offset bigint, bucket_count bigint, PRIMARY KEY (node_ip, histogram_id, bucket_offset)) WITH CLUSTERING ORDER BY (histogram_id DESC, bucket_offset ASC) AND" + PerformanceObjectsPlugin.getAdditionalTableOptions();
         Iterator var8 = GLOBAL_METRICS.iterator();

         while(var8.hasNext()) {
            String tableName = (String)var8.next();
            compile(tableName + "_global", globalMetrics);
         }

         compile("cluster_snapshot", "CREATE TABLE %s.%s (name text, datacenters set<text>, node_count int, keyspace_count int, table_count int, total_reads bigint, total_range_slices bigint, total_writes bigint, mean_read_latency double, mean_range_slice_latency double, mean_write_latency double, completed_mutations bigint,dropped_mutations bigint,dropped_mutation_ratio double,storage_capacity bigint, free_space bigint, table_data_size bigint, index_data_size bigint, compactions_completed bigint, compactions_pending int, read_requests_pending bigint, write_requests_pending bigint, read_repair_tasks_pending bigint, manual_repair_tasks_pending bigint, gossip_tasks_pending bigint, hinted_handoff_pending bigint, internal_responses_pending bigint, migrations_pending bigint, misc_tasks_pending bigint, request_responses_pending bigint, flush_sorter_tasks_pending bigint, memtable_post_flushers_pending bigint, replicate_on_write_tasks_pending bigint, streams_pending int, total_batches_replayed bigint, key_cache_entries bigint, key_cache_size bigint, key_cache_capacity bigint, row_cache_entries bigint, row_cache_size bigint, row_cache_capacity bigint, PRIMARY KEY (name)) WITH " + PerformanceObjectsPlugin.getAdditionalTableOptions());
         compile("dc_snapshot", "CREATE TABLE %s.%s (name text,node_count int,total_reads bigint, total_range_slices bigint, total_writes bigint, mean_read_latency double, mean_range_slice_latency double, mean_write_latency double, completed_mutations bigint,dropped_mutations bigint,dropped_mutation_ratio double,read_requests_pending bigint, write_requests_pending bigint, read_repair_tasks_pending bigint, manual_repair_tasks_pending bigint, gossip_tasks_pending bigint, hinted_handoff_pending bigint, internal_responses_pending bigint, migrations_pending bigint, misc_tasks_pending bigint, request_responses_pending bigint, flush_sorter_tasks_pending bigint, memtable_post_flushers_pending bigint, replicate_on_write_tasks_pending bigint, streams_pending int, storage_capacity bigint, free_space bigint, table_data_size bigint, index_data_size bigint, compactions_completed bigint, compactions_pending int, total_batches_replayed bigint, key_cache_entries bigint, key_cache_size bigint, key_cache_capacity bigint, row_cache_entries bigint, row_cache_size bigint, row_cache_capacity bigint, PRIMARY KEY (name)) WITH " + PerformanceObjectsPlugin.getAdditionalTableOptions());
         compile("dropped_messages", "CREATE TABLE IF NOT EXISTS %s.%s (node_ip inet, histogram_id timestamp, verb text, global_count bigint, global_mean_rate double, global_1min_rate double, global_5min_rate double, global_15min_rate double, internal_count bigint, internal_mean_rate double, internal_1min_rate double, internal_5min_rate double, internal_15min_rate double, internal_latency_median double, internal_latency_p75 double, internal_latency_p90 double, internal_latency_p95 double, internal_latency_p98 double, internal_latency_p99 double, internal_latency_min double, internal_latency_mean double, internal_latency_max double, internal_latency_stdev double, xnode_count bigint, xnode_mean_rate double, xnode_1min_rate double, xnode_5min_rate double, xnode_15min_rate double, xnode_median double, xnode_p75 double, xnode_p90 double, xnode_p95 double, xnode_p98 double, xnode_p99 double, xnode_min double, xnode_mean double, xnode_max double, xnode_stdev double, PRIMARY KEY (node_ip, histogram_id, verb)) WITH CLUSTERING ORDER BY (histogram_id DESC, verb ASC) AND " + PerformanceObjectsPlugin.getAdditionalTableOptions());
         compile("key_cache", "CREATE TABLE %s.%s (node_ip inet,cache_size bigint,cache_capacity bigint,cache_hits bigint,cache_requests bigint,hit_rate double,PRIMARY KEY (node_ip)) WITH " + PerformanceObjectsPlugin.getAdditionalTableOptions());
         compile("keyspace_snapshot", "CREATE TABLE %s.%s (keyspace_name text,total_reads bigint,total_writes bigint,mean_read_latency double,mean_write_latency double,total_data_size bigint,table_count int,index_count int,PRIMARY KEY (keyspace_name)) WITH " + PerformanceObjectsPlugin.getAdditionalTableOptions());
         compile("leases", "CREATE TABLE IF NOT EXISTS %s.%s(acquire_average_latency_ms bigint, acquire_latency99ms bigint, acquire_max_latency_ms bigint, acquire_rate15 double, dc text, disable_average_latency_ms bigint, disable_latency99ms bigint, disable_max_latency_ms bigint, disable_rate15 double, monitor inet, name text, renew_average_latency_ms bigint, renew_latency99ms bigint, renew_max_latency_ms bigint, renew_rate15 double, resolve_average_latency_ms bigint, resolve_latency99ms bigint, resolve_max_latency_ms bigint, resolve_rate15 double, up boolean, up_or_down_since timestamp,PRIMARY KEY((name, dc), monitor)) ");
         compile("net_stats", "CREATE TABLE %s.%s (node_ip inet,read_repair_attempted bigint,read_repaired_blocking bigint,read_repaired_background bigint,commands_pending int,commands_completed bigint,responses_pending int,responses_completed bigint, PRIMARY KEY (node_ip))WITH " + PerformanceObjectsPlugin.getAdditionalTableOptions());
         compile("node_slow_log", "CREATE TABLE %s.%s (node_ip inet,date timestamp,table_names set<text>,source_ip inet,username text,start_time timeuuid,duration bigint,commands list<text>,parameters map<text, text>,PRIMARY KEY ((node_ip, date), start_time)) WITH CLUSTERING ORDER BY (start_time DESC)  AND COMPACTION = {'class':'TimeWindowCompactionStrategy'}");
         compile("node_snapshot", "CREATE TABLE %s.%s (node_ip inet, state text, uptime bigint, tokens set<text>, data_owned float, datacenter text,rack text, total_reads bigint, total_range_slices bigint, total_writes bigint, mean_read_latency double, mean_range_slice_latency double, mean_write_latency double, read_timeouts bigint, range_slice_timeouts bigint, write_timeouts bigint, heap_total bigint, heap_used bigint, cms_collection_count bigint, cms_collection_time bigint, parnew_collection_count bigint, parnew_collection_time bigint, completed_mutations bigint,dropped_mutations bigint,dropped_mutation_ratio double,compactions_completed bigint, compactions_pending int, read_requests_pending bigint, write_requests_pending bigint, read_repair_tasks_pending bigint, manual_repair_tasks_pending bigint, gossip_tasks_pending bigint, hinted_handoff_pending bigint, internal_responses_pending bigint, migrations_pending bigint, misc_tasks_pending bigint, request_responses_pending bigint, flush_sorter_tasks_pending bigint, memtable_post_flushers_pending bigint, replicate_on_write_tasks_pending bigint, streams_pending int, storage_capacity bigint, free_space bigint, table_data_size bigint, index_data_size bigint, total_node_memory bigint, process_cpu_load double, total_batches_replayed bigint, key_cache_entries bigint, key_cache_size bigint, key_cache_capacity bigint, row_cache_entries bigint, row_cache_size bigint, row_cache_capacity bigint, commitlog_size bigint, commitlog_pending_tasks bigint, PRIMARY KEY (node_ip)) WITH " + PerformanceObjectsPlugin.getAdditionalTableOptions());
         compile("object_io", "CREATE TABLE %s.%s (node_ip inet,keyspace_name text,table_name text,read_latency double,total_reads bigint,write_latency double,total_writes bigint,memory_only boolean,last_activity timestamp,PRIMARY KEY(node_ip, keyspace_name, table_name)) WITH " + PerformanceObjectsPlugin.getAdditionalTableOptions());
         compile("object_read_io_snapshot", "CREATE TABLE %s.%s (node_ip inet,keyspace_name text,table_name text,read_latency double,total_reads bigint,write_latency double,total_writes bigint,memory_only boolean,latency_index int, PRIMARY KEY(node_ip, latency_index)) WITH CLUSTERING ORDER BY (latency_index ASC) AND  " + PerformanceObjectsPlugin.getAdditionalTableOptions());
         compile("object_write_io_snapshot", "CREATE TABLE %s.%s (node_ip inet,keyspace_name text,table_name text,read_latency double,total_reads bigint,write_latency double,total_writes bigint,memory_only boolean,latency_index int, PRIMARY KEY(node_ip, latency_index)) WITH CLUSTERING ORDER BY (latency_index ASC) AND " + PerformanceObjectsPlugin.getAdditionalTableOptions());
         compile("node_table_snapshot", "CREATE TABLE %s.%s (node_ip inet,keyspace_name text,table_name text,total_reads bigint,total_writes bigint,mean_read_latency double,mean_write_latency double,live_sstable_count bigint,bf_false_positives bigint,bf_false_positive_ratio double,key_cache_hit_rate double,compression_ratio double,droppable_tombstone_ratio double,memtable_size bigint,memtable_columns_count bigint,memtable_switch_count bigint,unleveled_sstables bigint,min_row_size bigint,max_row_size bigint,mean_row_size bigint,total_data_size bigint, PRIMARY KEY (node_ip, keyspace_name, table_name)) WITH " + PerformanceObjectsPlugin.getAdditionalTableOptions());
         compile("table_snapshot", "CREATE TABLE %s.%s (keyspace_name text,table_name text,total_reads bigint,total_writes bigint,mean_read_latency double,mean_write_latency double,live_sstable_count bigint,bf_false_positives bigint,bf_false_positive_ratio double,key_cache_hit_rate double,compression_ratio double,droppable_tombstone_ratio double,memtable_size bigint,memtable_columns_count bigint,memtable_switch_count bigint,unleveled_sstables bigint,min_row_size bigint,max_row_size bigint,mean_row_size bigint,total_data_size bigint, PRIMARY KEY (keyspace_name, table_name)) WITH " + PerformanceObjectsPlugin.getAdditionalTableOptions());
         compile("thread_pool_messages", "CREATE TABLE %s.%s (node_ip inet,message_type text,dropped_count int, PRIMARY KEY (node_ip, message_type)) WITH " + PerformanceObjectsPlugin.getAdditionalTableOptions());
         compile("thread_pool", "CREATE TABLE %s.%s (node_ip inet,pool_name text,active bigint,pending bigint,completed bigint,blocked bigint,all_time_blocked bigint, PRIMARY KEY (node_ip, pool_name))WITH " + PerformanceObjectsPlugin.getAdditionalTableOptions());
         compile("user_object_read_io_snapshot", "CREATE TABLE %s.%s (    keyspace_name text,    table_name text,    node_ip inet,    user_ip inet,    conn_id text,    username text,    read_latency double,    total_reads bigint,    write_latency double,    total_writes bigint,    read_quantiles map<double, double>,     write_quantiles map<double, double>,     latency_index int,  PRIMARY KEY (node_ip, latency_index))    WITH CLUSTERING ORDER BY (latency_index ASC)    AND " + PerformanceObjectsPlugin.getAdditionalTableOptions());
         compile("object_user_read_io_snapshot", "CREATE TABLE %s.%s (    keyspace_name text,    table_name text,    node_ip inet,    user_ip inet,    conn_id text,    username text,    read_latency double,    total_reads bigint,    write_latency double,    total_writes bigint,    read_quantiles map<double, double>,     write_quantiles map<double, double>,     latency_index int,PRIMARY KEY (node_ip, latency_index))    WITH CLUSTERING ORDER BY (latency_index ASC)    AND " + PerformanceObjectsPlugin.getAdditionalTableOptions());
         compile("user_read_io_snapshot", "CREATE TABLE %s.%s (    node_ip inet,    user_ip inet,    conn_id text,    username text,    read_latency double,    total_reads bigint,    write_latency double,    total_writes bigint,    latency_index int, PRIMARY KEY (node_ip, latency_index))    WITH CLUSTERING ORDER BY (latency_index ASC)    AND " + PerformanceObjectsPlugin.getAdditionalTableOptions());
         compile("object_user_write_io_snapshot", "CREATE TABLE %s.%s (    keyspace_name text,    table_name text,    node_ip inet,    user_ip inet,    conn_id text,    username text,    read_latency double,    total_reads bigint,    write_latency double,    total_writes bigint,    read_quantiles map<double, double>,     write_quantiles map<double, double>,     latency_index int, PRIMARY KEY (node_ip, latency_index))    WITH CLUSTERING ORDER BY (latency_index ASC)    AND " + PerformanceObjectsPlugin.getAdditionalTableOptions());
         compile("user_object_write_io_snapshot", "CREATE TABLE %s.%s (    keyspace_name text,    table_name text,    node_ip inet,    user_ip inet,    conn_id text,    username text,    read_latency double,    total_reads bigint,    write_latency double,    total_writes bigint,    read_quantiles map<double, double>,     write_quantiles map<double, double>,     latency_index int, PRIMARY KEY (node_ip, latency_index))    WITH CLUSTERING ORDER BY (latency_index ASC)    AND " + PerformanceObjectsPlugin.getAdditionalTableOptions());
         compile("user_write_io_snapshot", "CREATE TABLE %s.%s (    node_ip inet,    user_ip inet,    conn_id text,    username text,    read_latency double,    total_reads bigint,    write_latency double,    total_writes bigint,    latency_index int, PRIMARY KEY (node_ip, latency_index))    WITH CLUSTERING ORDER BY (latency_index ASC)    AND " + PerformanceObjectsPlugin.getAdditionalTableOptions());
         compile("user_object_io", "CREATE TABLE %s.%s (    keyspace_name text,    table_name text,    node_ip inet,    user_ip inet,    conn_id text,    username text,    read_latency double,    total_reads bigint,    write_latency double,    total_writes bigint,    read_quantiles map<double, double>,     write_quantiles map<double, double>,     last_activity timestamp, PRIMARY KEY ((node_ip, conn_id), keyspace_name, table_name))     WITH " + PerformanceObjectsPlugin.getAdditionalTableOptions());
         compile("object_user_io", "CREATE TABLE %s.%s (    keyspace_name text,    table_name text,    node_ip inet,    user_ip inet,    conn_id text,    username text,    read_latency double,    total_reads bigint,    write_latency double,    total_writes bigint,    read_quantiles map<double, double>,     write_quantiles map<double, double>,     last_activity timestamp, PRIMARY KEY ((node_ip, keyspace_name), table_name, conn_id))     WITH " + PerformanceObjectsPlugin.getAdditionalTableOptions());
         compile("user_io", "CREATE TABLE %s.%s (    node_ip inet,    user_ip inet,    conn_id text,    username text,    read_latency double,    total_reads bigint,    write_latency double,    total_writes bigint,    last_activity timestamp, PRIMARY KEY (node_ip, conn_id))    WITH " + PerformanceObjectsPlugin.getAdditionalTableOptions());
         if(extraTables != null) {
            extraTables.forEach((tableDef) -> {
               compile(tableDef.getTableName(), tableDef.getSchema());
            });
         }

         initialized.countDown();
      }
   }

   private static TableMetadata compile(String name, String description, String cql) {
      return CreateTableStatement.parse(String.format(cql, new Object[]{name}), "dse_perf").id(SchemaTool.tableIdForDseSystemTable("dse_perf", name)).comment(description).dcLocalReadRepairChance(0.0D).memtableFlushPeriod((int)TimeUnit.HOURS.toMillis(1L)).gcGraceSeconds((int)TimeUnit.MINUTES.toSeconds(1L)).build();
   }

   public static TableMetadata compile(String tableName, String schema) {
      TableMetadata metaData = compile(tableName, "", String.format(schema, new Object[]{"dse_perf", tableName}));
      tables.put(tableName, metaData);
      return metaData;
   }

   public static void maybeConfigure() {
      try {
         initialized.await();
         SchemaTool.maybeCreateOrUpdateKeyspace(metadata(), 0L);
      } catch (InterruptedException var1) {
         var1.printStackTrace(System.err);
      }

   }

   public static TableMetadata maybeCreateTable(String tableName) {
      try {
         initialized.await();
         maybeConfigure();
         TableMetadata existingTable = Schema.instance.getTableMetadata("dse_perf", tableName);
         if(existingTable == null) {
            TableMetadata cfm = (TableMetadata)tables.get(tableName);

            assert cfm != null : "Undefined table: " + tableName;

            logger.info("Creating a new table {}", tableName);
            return SchemaTool.maybeCreateTable("dse_perf", tableName, cfm);
         } else {
            logger.info("Using the existing table {}", tableName);
            return existingTable;
         }
      } catch (InterruptedException var3) {
         logger.warn("Interrupted while waiting for schema creation", var3);
         throw new RuntimeException(var3);
      }
   }

   public static TableMetadata maybeCreateTable(String tableName, String schema) {
      try {
         initialized.await();
         TableMetadata result = (TableMetadata)tables.get(tableName);
         if(result == null) {
            result = SchemaTool.maybeCreateTable("dse_perf", tableName, compile(tableName, schema));
            tables.put(tableName, result);
         }

         return result;
      } catch (InterruptedException var3) {
         logger.warn("Interrupted while waiting for schema creation", var3);
         throw new RuntimeException(var3);
      }
   }

   public static KeyspaceMetadata metadata() {
      return KeyspaceMetadata.create("dse_perf", KeyspaceParams.simple(1));
   }

   static {
      KEYSPACE_METRICS = ImmutableList.builder().add("sstables_per_read_histograms").addAll(GLOBAL_METRICS).build();
      TABLE_METRICS = ImmutableList.builder().add(new String[]{"cell_count_histograms", "partition_size_histograms"}).addAll(KEYSPACE_METRICS).build();
      SCHEMA_ADD_NODE_SLOW_LOG_TRACING_SESSION_ID = String.format("ALTER TABLE %s.%s ADD %s uuid;", new Object[]{"dse_perf", "node_slow_log", "tracing_session_id"});
      NODE_SNAPSHOT_ADD_BACKGROUND_IO_PENDING = String.format("ALTER TABLE %s.%s ADD %s bigint;", new Object[]{"dse_perf", "node_snapshot", "background_io_pending"});
      CLUSTER_SNAPSHOT_ADD_BACKGROUND_IO_PENDING = String.format("ALTER TABLE %s.%s ADD %s bigint;", new Object[]{"dse_perf", "cluster_snapshot", "background_io_pending"});
      DC_SNAPSHOT_ADD_BACKGROUND_IO_PENDING = String.format("ALTER TABLE %s.%s ADD %s bigint;", new Object[]{"dse_perf", "dc_snapshot", "background_io_pending"});
      tables = new ConcurrentHashMap();
      initialized = new CountDownLatch(1);
      logger = LoggerFactory.getLogger(PerformanceObjectsKeyspace.class);
   }

   public static class TableDef {
      private final String tableName;
      private final String schema;

      public TableDef(String tableName, String schema) {
         assert tableName != null;

         this.tableName = tableName;
         this.schema = schema;
      }

      public String getSchema() {
         return this.schema;
      }

      public String getTableName() {
         return this.tableName;
      }

      public boolean equals(Object other) {
         return this == other || other != null && this.getClass() == other.getClass() && this.tableName.equals(((PerformanceObjectsKeyspace.TableDef)other).tableName);
      }

      public int hashCode() {
         return this.tableName.hashCode();
      }
   }
}
