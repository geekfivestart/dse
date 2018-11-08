package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "gcstats",
   description = "Print GC Statistics"
)
public class GcStats extends NodeTool.NodeToolCmd {
   public GcStats() {
   }

   public void execute(NodeProbe probe) {
      double[] stats = probe.getAndResetGCStats();
      double mean = stats[2] / stats[5];
      double stdev = Math.sqrt(stats[3] / stats[5] - mean * mean);
      System.out.printf("%20s%20s%20s%20s%20s%20s%25s%n", new Object[]{"Interval (ms)", "Max GC Elapsed (ms)", "Total GC Elapsed (ms)", "Stdev GC Elapsed (ms)", "GC Reclaimed (MB)", "Collections", "Direct Memory Bytes"});
      System.out.printf("%20.0f%20.0f%20.0f%20.0f%20.0f%20.0f%25d%n", new Object[]{Double.valueOf(stats[0]), Double.valueOf(stats[1]), Double.valueOf(stats[2]), Double.valueOf(stdev), Double.valueOf(stats[4]), Double.valueOf(stats[5]), Long.valueOf((long)stats[6])});
   }
}
