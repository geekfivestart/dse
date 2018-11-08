package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "setcompactionthroughput",
   description = "Set the MB/s throughput cap for compaction in the system, or 0 to disable throttling"
)
public class SetCompactionThroughput extends NodeTool.NodeToolCmd {
   @Arguments(
      title = "compaction_throughput",
      usage = "<value_in_mb>",
      description = "Value in MB, 0 to disable throttling",
      required = true
   )
   private Integer compactionThroughput = null;

   public SetCompactionThroughput() {
   }

   public void execute(NodeProbe probe) {
      probe.setCompactionThroughput(this.compactionThroughput.intValue());
   }
}
