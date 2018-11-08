package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "enablebinary",
   description = "Reenable native transport (binary protocol)"
)
public class EnableBinary extends NodeTool.NodeToolCmd {
   public EnableBinary() {
   }

   public void execute(NodeProbe probe) {
      probe.startNativeTransport();
   }
}
