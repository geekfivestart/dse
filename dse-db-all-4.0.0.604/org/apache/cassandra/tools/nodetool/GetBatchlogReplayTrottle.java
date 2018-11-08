package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "getbatchlogreplaythrottle",
   description = "Print batchlog replay throttle in KB/s. This is reduced proportionally to the number of nodes in the cluster."
)
public class GetBatchlogReplayTrottle extends NodeTool.NodeToolCmd {
   public GetBatchlogReplayTrottle() {
   }

   public void execute(NodeProbe probe) {
      System.out.println("Batchlog replay throttle: " + probe.getBatchlogReplayThrottle() + " KB/s");
   }
}
