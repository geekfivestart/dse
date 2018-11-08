package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "enablehandoff",
   description = "Reenable future hints storing on the current node"
)
public class EnableHandoff extends NodeTool.NodeToolCmd {
   public EnableHandoff() {
   }

   public void execute(NodeProbe probe) {
      probe.enableHintedHandoff();
   }
}
