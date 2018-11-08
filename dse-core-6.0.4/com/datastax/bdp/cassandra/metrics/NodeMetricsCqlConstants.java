package com.datastax.bdp.cassandra.metrics;

public class NodeMetricsCqlConstants {
   public static final String OBJECT_READ_IO_INSERT = String.format("INSERT INTO %s.%s (node_ip,keyspace_name,table_name,read_latency,total_reads,write_latency,total_writes,memory_only,latency_index) VALUES(?,?,?,?,?,?,?,?,?) USING TTL ?;", new Object[]{"dse_perf", "object_read_io_snapshot"});
   public static final String OBJECT_WRITE_IO_INSERT = String.format("INSERT INTO %s.%s (node_ip,keyspace_name,table_name,read_latency,total_reads,write_latency,total_writes,memory_only,latency_index)VALUES(?,?,?,?,?,?,?,?,?) USING TTL ?;", new Object[]{"dse_perf", "object_write_io_snapshot"});
   public static final String OBJECT_IO_INSERT = String.format("INSERT INTO %s.%s (node_ip,keyspace_name,table_name,read_latency,total_reads,write_latency,total_writes,memory_only,last_activity)VALUES(?,?,?,?,?,?,?,?,?) USING TTL ?;", new Object[]{"dse_perf", "object_io"});

   public NodeMetricsCqlConstants() {
   }
}
