package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "statusbinary",
   description = "Status of native transport (binary protocol)"
)
public class StatusBinary extends NodeTool.NodeToolCmd {
   public StatusBinary() {
   }

   public void execute(NodeProbe probe) {
      System.out.println(probe.isNativeTransportRunning()?"running":"not running");
   }
}
