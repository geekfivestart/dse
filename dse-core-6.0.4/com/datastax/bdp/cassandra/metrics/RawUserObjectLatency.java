package com.datastax.bdp.cassandra.metrics;

import com.datastax.bdp.cassandra.tracing.ClientConnectionMetadata;
import java.util.Comparator;

public class RawUserObjectLatency {
   public static final Comparator<RawUserObjectLatency> READ_LATENCY_COMPARATOR = new Comparator<RawUserObjectLatency>() {
      public int compare(RawUserObjectLatency o1, RawUserObjectLatency o2) {
         return RawObjectLatency.READ_LATENCY_COMPARATOR.compare(o1.latency, o2.latency);
      }
   };
   public static final Comparator<RawUserObjectLatency> WRITE_LATENCY_COMPARATOR = new Comparator<RawUserObjectLatency>() {
      public int compare(RawUserObjectLatency o1, RawUserObjectLatency o2) {
         return RawObjectLatency.WRITE_LATENCY_COMPARATOR.compare(o1.latency, o2.latency);
      }
   };
   public final ClientConnectionMetadata connectionInfo;
   public final RawObjectLatency latency;

   public RawUserObjectLatency(ClientConnectionMetadata connectionInfo, RawObjectLatency latency) {
      this.connectionInfo = connectionInfo;
      this.latency = latency;
   }

   public String toString() {
      return String.format("RawUserObjectLatency : { session: %s, object: %s.%s, totalReadLatency: %s, totalReads: %s, totalWriteLatency: %s, totalWrites: %s }", new Object[]{this.connectionInfo, this.latency.keyspace, this.latency.columnFamily, Long.valueOf(this.latency.totalReadLatency), Long.valueOf(this.latency.totalReads), Long.valueOf(this.latency.totalWriteLatency), Long.valueOf(this.latency.totalWrites)});
   }
}
