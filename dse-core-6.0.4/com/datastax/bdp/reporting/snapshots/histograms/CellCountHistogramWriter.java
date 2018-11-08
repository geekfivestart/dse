package com.datastax.bdp.reporting.snapshots.histograms;

import java.net.InetAddress;

public class CellCountHistogramWriter extends AbstractHistogramWriter {
   public CellCountHistogramWriter(InetAddress nodeAddress, int ttl) {
      super(nodeAddress, ttl);
   }

   protected String getTableName() {
      return "cell_count_histograms";
   }
}
