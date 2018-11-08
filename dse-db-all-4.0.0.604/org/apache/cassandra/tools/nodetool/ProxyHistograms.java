package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "proxyhistograms",
   description = "Print statistic histograms for network operations"
)
public class ProxyHistograms extends NodeTool.NodeToolCmd {
   public ProxyHistograms() {
   }

   public void execute(NodeProbe probe) {
      String[] percentiles = new String[]{"50%", "75%", "95%", "98%", "99%", "Min", "Max"};
      double[] readLatency = probe.metricPercentilesAsArray(probe.getProxyMetric("Read"));
      double[] writeLatency = probe.metricPercentilesAsArray(probe.getProxyMetric("Write"));
      double[] rangeLatency = probe.metricPercentilesAsArray(probe.getProxyMetric("RangeSlice"));
      double[] casReadLatency = probe.metricPercentilesAsArray(probe.getProxyMetric("CASRead"));
      double[] casWriteLatency = probe.metricPercentilesAsArray(probe.getProxyMetric("CASWrite"));
      double[] viewWriteLatency = probe.metricPercentilesAsArray(probe.getProxyMetric("ViewWrite"));
      System.out.println("proxy histograms");
      System.out.println(String.format("%-10s%19s%19s%19s%19s%19s%19s", new Object[]{"Percentile", "Read Latency", "Write Latency", "Range Latency", "CAS Read Latency", "CAS Write Latency", "View Write Latency"}));
      System.out.println(String.format("%-10s%19s%19s%19s%19s%19s%19s", new Object[]{"", "(micros)", "(micros)", "(micros)", "(micros)", "(micros)", "(micros)"}));

      for(int i = 0; i < percentiles.length; ++i) {
         System.out.println(String.format("%-10s%19.2f%19.2f%19.2f%19.2f%19.2f%19.2f", new Object[]{percentiles[i], Double.valueOf(readLatency[i]), Double.valueOf(writeLatency[i]), Double.valueOf(rangeLatency[i]), Double.valueOf(casReadLatency[i]), Double.valueOf(casWriteLatency[i]), Double.valueOf(viewWriteLatency[i])}));
      }

      System.out.println();
   }
}
