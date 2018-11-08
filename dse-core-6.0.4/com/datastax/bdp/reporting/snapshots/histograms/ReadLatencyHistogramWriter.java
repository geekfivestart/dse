package com.datastax.bdp.reporting.snapshots.histograms;

import java.net.InetAddress;

public class ReadLatencyHistogramWriter extends AbstractHistogramWriter {
   public ReadLatencyHistogramWriter(InetAddress nodeAddress, int ttl) {
      super(nodeAddress, ttl);
   }

   protected String getTableName() {
      return "read_latency_histograms";
   }
}
