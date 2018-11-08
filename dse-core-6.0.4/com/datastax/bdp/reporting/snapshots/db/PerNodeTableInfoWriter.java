package com.datastax.bdp.reporting.snapshots.db;

import com.datastax.bdp.reporting.CqlWriter;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.List;

public class PerNodeTableInfoWriter extends CqlWriter<TableInfo> {
   public static final String INSERT_CQL = String.format("INSERT INTO %s.%s (node_ip,keyspace_name,table_name,total_reads,total_writes,mean_read_latency,mean_write_latency,live_sstable_count,bf_false_positives,bf_false_positive_ratio,key_cache_hit_rate,compression_ratio,droppable_tombstone_ratio,memtable_size,memtable_columns_count,memtable_switch_count,unleveled_sstables,min_row_size,max_row_size,mean_row_size,total_data_size)VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) USING TTL ?", new Object[]{"dse_perf", "node_table_snapshot"});

   public PerNodeTableInfoWriter(InetAddress nodeAddress, int ttl) {
      super(nodeAddress, ttl);
   }

   public String getTableName() {
      return "node_table_snapshot";
   }

   public String getInsertCQL() {
      return String.format(INSERT_CQL, new Object[]{"dse_perf", this.getTableName()});
   }

   public List<ByteBuffer> getVariables(TableInfo tableInfo) {
      List<ByteBuffer> vars = tableInfo.toByteBufferList();
      vars.add(0, this.nodeAddressBytes);
      vars.add(this.getTtlBytes());
      return vars;
   }
}
