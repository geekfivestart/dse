package com.datastax.bdp.reporting.snapshots.histograms;

import java.net.InetAddress;

public class SSTablesPerReadHistogramWriter extends AbstractHistogramWriter {
   public SSTablesPerReadHistogramWriter(InetAddress nodeAddress, int ttl) {
      super(nodeAddress, ttl);
   }

   protected String getTableName() {
      return "sstables_per_read_histograms";
   }
}
