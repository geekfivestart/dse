package com.datastax.bdp.reporting.snapshots.histograms;

import java.net.InetAddress;

public class PartitionSizeHistogramWriter extends AbstractHistogramWriter {
   public PartitionSizeHistogramWriter(InetAddress nodeAddress, int ttl) {
      super(nodeAddress, ttl);
   }

   protected String getTableName() {
      return "partition_size_histograms";
   }
}
