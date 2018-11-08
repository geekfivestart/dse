package org.apache.cassandra.tools.nodetool;

import com.google.common.base.Preconditions;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import java.util.ArrayList;
import java.util.List;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "refresh",
   description = "Load newly placed SSTables to the system without restart"
)
public class Refresh extends NodeTool.NodeToolCmd {
   @Arguments(
      usage = "<keyspace> <table>",
      description = "The keyspace and table name"
   )
   private List<String> args = new ArrayList();
   @Option(
      title = "reset-levels",
      name = {"--reset-levels"},
      description = "Use --reset-levels to force all sstables to level 0"
   )
   private boolean resetLevels = false;

   public Refresh() {
   }

   public void execute(NodeProbe probe) {
      Preconditions.checkArgument(this.args.size() == 2, "refresh requires ks and cf args");
      probe.loadNewSSTables((String)this.args.get(0), (String)this.args.get(1), this.resetLevels);
   }
}
