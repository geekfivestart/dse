package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "reloadlocalschema",
   description = "Reload local node schema from system tables"
)
public class ReloadLocalSchema extends NodeTool.NodeToolCmd {
   public ReloadLocalSchema() {
   }

   public void execute(NodeProbe probe) {
      probe.reloadLocalSchema();
   }
}
