package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "enablegossip",
   description = "Reenable gossip"
)
public class EnableGossip extends NodeTool.NodeToolCmd {
   public EnableGossip() {
   }

   public void execute(NodeProbe probe) {
      probe.startGossiping();
   }
}
