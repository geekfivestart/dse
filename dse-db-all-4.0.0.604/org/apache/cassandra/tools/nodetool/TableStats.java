package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import java.util.ArrayList;
import java.util.List;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.nodetool.stats.StatsHolder;
import org.apache.cassandra.tools.nodetool.stats.StatsPrinter;
import org.apache.cassandra.tools.nodetool.stats.TableStatsHolder;
import org.apache.cassandra.tools.nodetool.stats.TableStatsPrinter;

@Command(
   name = "tablestats",
   description = "Print statistics on tables"
)
public class TableStats extends NodeTool.NodeToolCmd {
   @Arguments(
      usage = "[<keyspace.table>...]",
      description = "List of tables (or keyspace) names"
   )
   private List<String> tableNames = new ArrayList();
   @Option(
      name = {"-i"},
      description = "Ignore the list of tables and display the remaining tables"
   )
   private boolean ignore = false;
   @Option(
      title = "human_readable",
      name = {"-H", "--human-readable"},
      description = "Display bytes in human readable form, i.e. KiB, MiB, GiB, TiB"
   )
   private boolean humanReadable = false;
   @Option(
      title = "format",
      name = {"-F", "--format"},
      description = "Output format (json, yaml)"
   )
   private String outputFormat = "";

   public TableStats() {
   }

   public void execute(NodeProbe probe) {
      if(!this.outputFormat.isEmpty() && !"json".equals(this.outputFormat) && !"yaml".equals(this.outputFormat)) {
         throw new IllegalArgumentException("arguments for -F are json,yaml only.");
      } else {
         StatsHolder holder = new TableStatsHolder(probe, this.humanReadable, this.ignore, this.tableNames);
         StatsPrinter printer = TableStatsPrinter.from(this.outputFormat);
         printer.print(holder, System.out);
      }
   }
}
