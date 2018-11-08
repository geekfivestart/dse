package com.datastax.bdp.db.audit;

public class CassandraAuditWriterOptions {
   public String mode = "sync";
   public String batch_size = "50";
   public String flush_time = "500";
   public String queue_size = "10000";
   public String day_partition_millis = Integer.valueOf(3600000).toString();
   public String write_consistency = "QUORUM";
   public String dropped_event_log = "/var/log/cassandra/dropped_audit_events.log";

   public CassandraAuditWriterOptions() {
   }
}
