package com.datastax.bdp.reporting.snapshots.db;

import com.datastax.bdp.reporting.CqlWriter;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.List;

public class KeyspaceInfoWriter extends CqlWriter<KeyspaceInfo> {
   public static final String KEYSPACE_SNAPSHOT_INSERT = String.format("INSERT INTO %s.%s (keyspace_name,total_reads,total_writes,mean_read_latency,mean_write_latency,total_data_size,table_count,index_count )VALUES (?,?,?,?,?,?,?,?) USING TTL ?", new Object[]{"dse_perf", "keyspace_snapshot"});

   public KeyspaceInfoWriter(InetAddress nodeAddress, int ttl) {
      super(nodeAddress, ttl);
   }

   public String getTableName() {
      return "keyspace_snapshot";
   }

   public String getInsertCQL() {
      return KEYSPACE_SNAPSHOT_INSERT;
   }

   public List<ByteBuffer> getVariables(KeyspaceInfo keyspaceInfo) {
      List<ByteBuffer> vars = keyspaceInfo.toByteBufferList();
      vars.add(this.getTtlBytes());
      return vars;
   }
}
