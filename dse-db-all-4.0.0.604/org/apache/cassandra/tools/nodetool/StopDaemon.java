package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.utils.JVMStabilityInspector;

@Command(
   name = "stopdaemon",
   description = "Stop DSE daemon"
)
public class StopDaemon extends NodeTool.NodeToolCmd {
   public StopDaemon() {
   }

   public void execute(NodeProbe probe) {
      try {
         DatabaseDescriptor.toolInitialization();
         probe.stopCassandraDaemon();
      } catch (Exception var3) {
         JVMStabilityInspector.inspectThrowable(var3);
      }

   }
}
