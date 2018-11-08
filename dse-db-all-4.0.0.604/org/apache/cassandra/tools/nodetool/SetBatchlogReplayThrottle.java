package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "setbatchlogreplaythrottle",
   description = "Set batchlog replay throttle in KB per second, or 0 to disable throttling. This will be reduced proportionally to the number of nodes in the cluster."
)
public class SetBatchlogReplayThrottle extends NodeTool.NodeToolCmd {
   @Arguments(
      title = "batchlog_replay_throttle",
      usage = "<value_in_kb_per_sec>",
      description = "Value in KB per second, 0 to disable throttling",
      required = true
   )
   private Integer batchlogReplayThrottle = null;

   public SetBatchlogReplayThrottle() {
   }

   public void execute(NodeProbe probe) {
      probe.setBatchlogReplayThrottle(this.batchlogReplayThrottle.intValue());
   }
}
