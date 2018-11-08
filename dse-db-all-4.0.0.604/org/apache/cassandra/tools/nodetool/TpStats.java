package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.nodetool.stats.StatsHolder;
import org.apache.cassandra.tools.nodetool.stats.StatsPrinter;
import org.apache.cassandra.tools.nodetool.stats.TpStatsHolder;
import org.apache.cassandra.tools.nodetool.stats.TpStatsPrinter;

@Command(
   name = "tpstats",
   description = "Print usage statistics of thread pools"
)
public class TpStats extends NodeTool.NodeToolCmd {
   @Option(
      title = "format",
      name = {"-F", "--format"},
      description = "Output format (json, yaml)"
   )
   private String outputFormat = "";
   @Option(
      title = "cores",
      name = {"-C", "--cores"},
      description = "Include TPC core data"
   )
   private boolean includeTPCCores = false;

   public TpStats() {
   }

   public void execute(NodeProbe probe) {
      if(!this.outputFormat.isEmpty() && !"json".equals(this.outputFormat) && !"yaml".equals(this.outputFormat)) {
         throw new IllegalArgumentException("arguments for -F are json,yaml only.");
      } else {
         StatsHolder data = new TpStatsHolder(probe, this.includeTPCCores);
         StatsPrinter printer = TpStatsPrinter.from(this.outputFormat);
         printer.print(data, System.out);
      }
   }
}
