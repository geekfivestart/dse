package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "disablehandoff",
   description = "Disable storing hinted handoffs"
)
public class DisableHandoff extends NodeTool.NodeToolCmd {
   public DisableHandoff() {
   }

   public void execute(NodeProbe probe) {
      probe.disableHintedHandoff();
   }
}
