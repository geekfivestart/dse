package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "refreshsizeestimates",
   description = "Refresh system.size_estimates"
)
public class RefreshSizeEstimates extends NodeTool.NodeToolCmd {
   public RefreshSizeEstimates() {
   }

   public void execute(NodeProbe probe) {
      probe.refreshSizeEstimates();
   }
}
