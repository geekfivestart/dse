package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "pausehandoff",
   description = "Pause hints delivery process"
)
public class PauseHandoff extends NodeTool.NodeToolCmd {
   public PauseHandoff() {
   }

   public void execute(NodeProbe probe) {
      probe.pauseHintsDelivery();
   }
}
