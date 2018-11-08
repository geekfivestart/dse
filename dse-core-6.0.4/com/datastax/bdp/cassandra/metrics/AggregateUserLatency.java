package com.datastax.bdp.cassandra.metrics;

import com.datastax.bdp.cassandra.tracing.ClientConnectionMetadata;
import java.util.Comparator;

public class AggregateUserLatency {
   public static final Comparator<AggregateUserLatency> AGGREGATE_READ_LATENCY_COMPARATOR = new Comparator<AggregateUserLatency>() {
      public int compare(AggregateUserLatency o1, AggregateUserLatency o2) {
         return Double.compare(o2.getMeanReadLatency(), o1.getMeanReadLatency());
      }
   };
   public static final Comparator<AggregateUserLatency> AGGREGATE_WRITE_LATENCY_COMPARATOR = new Comparator<AggregateUserLatency>() {
      public int compare(AggregateUserLatency o1, AggregateUserLatency o2) {
         return Double.compare(o2.getMeanWriteLatency(), o1.getMeanWriteLatency());
      }
   };
   public final ClientConnectionMetadata connectionInfo;
   private long readLatency = 0L;
   private long readOps = 0L;
   private long writeLatency = 0L;
   private long writeOps = 0L;

   public AggregateUserLatency(ClientConnectionMetadata connectionMetadata) {
      this.connectionInfo = connectionMetadata;
   }

   public void addLatencyMetric(RawObjectLatency latency) {
      this.readLatency += latency.totalReadLatency;
      this.readOps += latency.totalReads;
      this.writeLatency += latency.totalWriteLatency;
      this.writeOps += latency.totalWrites;
   }

   public long getReadOps() {
      return this.readOps;
   }

   public long getWriteOps() {
      return this.writeOps;
   }

   public double getMeanReadLatency() {
      return this.readOps == 0L?0.0D:(double)this.readLatency / (double)this.readOps;
   }

   public double getMeanWriteLatency() {
      return this.writeOps == 0L?0.0D:(double)this.writeLatency / (double)this.writeOps;
   }

   public String toString() {
      return String.format("AggregateUserLatency : { session: %s, totalReadLatency: %s, totalReads: %s, totalWriteLatency: %s, totalWrites: %s}", new Object[]{this.connectionInfo, Long.valueOf(this.readLatency), Long.valueOf(this.readOps), Long.valueOf(this.writeLatency), Long.valueOf(this.writeOps)});
   }
}
