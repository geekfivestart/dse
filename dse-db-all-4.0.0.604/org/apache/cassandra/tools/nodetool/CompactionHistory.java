package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.nodetool.stats.CompactionHistoryHolder;
import org.apache.cassandra.tools.nodetool.stats.CompactionHistoryPrinter;
import org.apache.cassandra.tools.nodetool.stats.StatsHolder;
import org.apache.cassandra.tools.nodetool.stats.StatsPrinter;

@Command(
   name = "compactionhistory",
   description = "Print history of compaction"
)
public class CompactionHistory extends NodeTool.NodeToolCmd {
   @Option(
      title = "format",
      name = {"-F", "--format"},
      description = "Output format (json, yaml)"
   )
   private String outputFormat = "";

   public CompactionHistory() {
   }

   public void execute(NodeProbe probe) {
      if(!this.outputFormat.isEmpty() && !"json".equals(this.outputFormat) && !"yaml".equals(this.outputFormat)) {
         throw new IllegalArgumentException("arguments for -F are json,yaml only.");
      } else {
         StatsHolder data = new CompactionHistoryHolder(probe);
         StatsPrinter printer = CompactionHistoryPrinter.from(this.outputFormat);
         printer.print(data, System.out);
      }
   }
}
