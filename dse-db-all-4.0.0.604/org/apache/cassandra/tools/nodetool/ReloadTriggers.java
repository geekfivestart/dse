package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "reloadtriggers",
   description = "Reload trigger classes"
)
public class ReloadTriggers extends NodeTool.NodeToolCmd {
   public ReloadTriggers() {
   }

   public void execute(NodeProbe probe) {
      probe.reloadTriggers();
   }
}
