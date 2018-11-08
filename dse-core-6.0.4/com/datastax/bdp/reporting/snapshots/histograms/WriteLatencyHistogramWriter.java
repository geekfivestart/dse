package com.datastax.bdp.reporting.snapshots.histograms;

import java.net.InetAddress;

public class WriteLatencyHistogramWriter extends AbstractHistogramWriter {
   public WriteLatencyHistogramWriter(InetAddress nodeAddress, int ttl) {
      super(nodeAddress, ttl);
   }

   protected String getTableName() {
      return "write_latency_histograms";
   }
}
