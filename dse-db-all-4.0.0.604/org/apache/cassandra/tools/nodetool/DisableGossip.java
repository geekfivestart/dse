package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "disablegossip",
   description = "Disable gossip (effectively marking the node down)"
)
public class DisableGossip extends NodeTool.NodeToolCmd {
   public DisableGossip() {
   }

   public void execute(NodeProbe probe) {
      probe.stopGossiping();
   }
}
