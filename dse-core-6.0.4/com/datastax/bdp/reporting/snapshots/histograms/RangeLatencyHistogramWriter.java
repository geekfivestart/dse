package com.datastax.bdp.reporting.snapshots.histograms;

import java.net.InetAddress;

public class RangeLatencyHistogramWriter extends AbstractHistogramWriter {
   public RangeLatencyHistogramWriter(InetAddress nodeAddress, int ttl) {
      super(nodeAddress, ttl);
   }

   protected String getTableName() {
      return "range_latency_histograms";
   }
}
