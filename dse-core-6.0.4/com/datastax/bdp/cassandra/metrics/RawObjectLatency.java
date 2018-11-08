package com.datastax.bdp.cassandra.metrics;

import com.datastax.bdp.util.QuantileEstimator;
import java.util.Comparator;
import java.util.SortedMap;

public class RawObjectLatency {
   public static final Comparator<RawObjectLatency> READ_LATENCY_COMPARATOR = new Comparator<RawObjectLatency>() {
      public int compare(RawObjectLatency o1, RawObjectLatency o2) {
         return Double.compare(o2.getMeanReadLatency(), o1.getMeanReadLatency());
      }
   };
   public static final Comparator<RawObjectLatency> WRITE_LATENCY_COMPARATOR = new Comparator<RawObjectLatency>() {
      public int compare(RawObjectLatency o1, RawObjectLatency o2) {
         return Double.compare(o2.getMeanWriteLatency(), o1.getMeanWriteLatency());
      }
   };
   public static final Comparator<RawObjectLatency> READ_HIGH_LATENCY_COMPARATOR = new Comparator<RawObjectLatency>() {
      public int compare(RawObjectLatency o1, RawObjectLatency o2) {
         return QuantileEstimator.compareByMaxQuantile(o2.readQuantiles, o1.readQuantiles);
      }
   };
   public static final Comparator<RawObjectLatency> WRITE_HIGH_LATENCY_COMPARATOR = new Comparator<RawObjectLatency>() {
      public int compare(RawObjectLatency o1, RawObjectLatency o2) {
         return QuantileEstimator.compareByMaxQuantile(o2.writeQuantiles, o1.writeQuantiles);
      }
   };
   public final String keyspace;
   public final String columnFamily;
   public final long totalWriteLatency;
   public final long totalWrites;
   public final long totalReadLatency;
   public final long totalReads;
   public final SortedMap<Double, Double> readQuantiles;
   public final SortedMap<Double, Double> writeQuantiles;

   public RawObjectLatency(String keyspace, String columnFamily, long totalReadLatency, long totalReads, long totalWriteLatency, long totalWrites, SortedMap<Double, Double> readQuantiles, SortedMap<Double, Double> writeQuantiles) {
      this.keyspace = keyspace;
      this.columnFamily = columnFamily;
      this.totalReadLatency = totalReadLatency;
      this.totalReads = totalReads;
      this.totalWriteLatency = totalWriteLatency;
      this.totalWrites = totalWrites;
      this.readQuantiles = readQuantiles;
      this.writeQuantiles = writeQuantiles;
   }

   public double getMeanReadLatency() {
      return this.totalReads == 0L?0.0D:(double)this.totalReadLatency / (double)this.totalReads;
   }

   public double getMeanWriteLatency() {
      return this.totalWrites == 0L?0.0D:(double)this.totalWriteLatency / (double)this.totalWrites;
   }

   public String toString() {
      return String.format("RawObjectLatency : { object: %s.%s, totalReadLatency: %s, totalReads: %s, totalWriteLatency: %s, totalWrites: %s, readQuantiles: %s, writeQuantiles: %s}", new Object[]{this.keyspace, this.columnFamily, Long.valueOf(this.totalReadLatency), Long.valueOf(this.totalReads), Long.valueOf(this.totalWriteLatency), Long.valueOf(this.totalWrites), this.readQuantiles.toString(), this.writeQuantiles.toString()});
   }
}
