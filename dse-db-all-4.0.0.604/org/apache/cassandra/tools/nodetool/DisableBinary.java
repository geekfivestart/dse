package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "disablebinary",
   description = "Disable native transport (binary protocol)"
)
public class DisableBinary extends NodeTool.NodeToolCmd {
   public DisableBinary() {
   }

   public void execute(NodeProbe probe) {
      probe.stopNativeTransport();
   }
}
