package com.datastax.bdp.reporting.snapshots.node;

import com.datastax.bdp.gms.DseVersionNotifier;
import com.datastax.bdp.reporting.CqlWriter;
import com.datastax.bdp.server.SystemInfo;
import com.datastax.bdp.system.PerformanceObjectsKeyspace;
import com.datastax.bdp.util.SchemaTool;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraVersion;

public class ClusterInfoWriter extends CqlWriter<ClusterInfo> {
   private static final List<String> columns_5_1 = Arrays.asList(new String[]{"name", "datacenters", "keyspace_count", "table_count", "node_count", "total_reads", "total_range_slices", "total_writes", "mean_read_latency", "mean_range_slice_latency", "mean_write_latency", "completed_mutations", "dropped_mutations", "dropped_mutation_ratio", "storage_capacity", "free_space", "table_data_size", "index_data_size", "compactions_completed", "compactions_pending", "read_requests_pending", "write_requests_pending", "read_repair_tasks_pending", "manual_repair_tasks_pending", "gossip_tasks_pending", "hinted_handoff_pending", "internal_responses_pending", "migrations_pending", "misc_tasks_pending", "request_responses_pending", "flush_sorter_tasks_pending", "memtable_post_flushers_pending", "replicate_on_write_tasks_pending", "streams_pending", "total_batches_replayed", "key_cache_entries", "key_cache_size", "key_cache_capacity", "row_cache_entries", "row_cache_size", "row_cache_capacity"});
   private static final List<String> columns_6_0;
   private final ByteBuffer clusterName;

   public ClusterInfoWriter(InetAddress nodeAddress, int ttl, String clusterName) {
      super(nodeAddress, ttl);
      this.clusterName = ByteBufferUtil.bytes(clusterName);
   }

   protected String getTableName() {
      return "cluster_snapshot";
   }

   protected String getInsertCQL() {
      return this.getInsertCQL(SystemInfo.getDseVersion());
   }

   private String getInsertCQL(CassandraVersion cassandraVersion) {
      List columns;
      if(cassandraVersion.compareTo(DseVersionNotifier.VERSION_60) >= 0) {
         columns = columns_6_0;
      } else {
         columns = columns_5_1;
      }

      return String.format("INSERT INTO %s.%s (%s) VALUES (%s) USING TTL ?", new Object[]{"dse_perf", "cluster_snapshot", columns.stream().collect(Collectors.joining(",")), columns.stream().map((c) -> {
         return "?";
      }).collect(Collectors.joining(","))});
   }

   protected List<ByteBuffer> getVariables(ClusterInfo writeable) {
      return this.getVariables(writeable, SystemInfo.getDseVersion());
   }

   private List<ByteBuffer> getVariables(ClusterInfo clusterInfo, CassandraVersion cassandraVersion) {
      List<ByteBuffer> vars = clusterInfo.toByteBufferList(cassandraVersion);
      vars.add(0, this.clusterName);
      vars.add(this.getTtlBytes());
      return vars;
   }

   protected CqlWriter<ClusterInfo>.WriterConfig createWriterConfig(CassandraVersion dseVersion) {
      return new CqlWriter<ClusterInfo>.WriterConfig(this.getInsertCQL(dseVersion), (clusterInfo) -> {
         return this.getVariables(clusterInfo, dseVersion);
      });
   }

   public void maybeAlterSchema() {
      SchemaTool.maybeAddNewColumn("dse_perf", "cluster_snapshot", "background_io_pending", PerformanceObjectsKeyspace.CLUSTER_SNAPSHOT_ADD_BACKGROUND_IO_PENDING);
   }

   static {
      columns_6_0 = new ArrayList(columns_5_1);
      columns_6_0.addAll(Arrays.asList(new String[]{"background_io_pending"}));
   }
}
