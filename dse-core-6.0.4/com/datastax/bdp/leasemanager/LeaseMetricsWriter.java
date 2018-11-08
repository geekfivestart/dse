package com.datastax.bdp.leasemanager;

import com.datastax.bdp.config.DseConfig;
import com.datastax.bdp.reporting.CqlWriter;
import com.datastax.bdp.util.Addresses;
import com.datastax.bdp.util.genericql.ObjectSerializer;
import java.nio.ByteBuffer;
import java.util.List;

public class LeaseMetricsWriter extends CqlWriter<LeaseMetrics> {
   private final ObjectSerializer<LeaseMetrics> serializer = new ObjectSerializer(LeaseMetrics.class);

   public LeaseMetricsWriter() {
      super(Addresses.Internode.getBroadcastAddress(), DseConfig.getLeaseMetricsTtl());
   }

   protected String getTableName() {
      return "leases";
   }

   protected String getInsertCQL() {
      return String.format("INSERT INTO %s.%s (acquire_average_latency_ms, acquire_latency99ms, acquire_max_latency_ms, acquire_rate15, dc, disable_average_latency_ms, disable_latency99ms, disable_max_latency_ms, disable_rate15, monitor, name, renew_average_latency_ms, renew_latency99ms, renew_max_latency_ms, renew_rate15, resolve_average_latency_ms, resolve_latency99ms, resolve_max_latency_ms, resolve_rate15, up, up_or_down_since)VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL ? ", new Object[]{"dse_perf", this.getTableName()});
   }

   protected List<ByteBuffer> getVariables(LeaseMetrics leaseMetricsSnapshot) {
      List<ByteBuffer> variables = this.serializer.toByteBufferList(leaseMetricsSnapshot);
      variables.add(this.getTtlBytes());
      return variables;
   }
}
