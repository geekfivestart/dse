package org.apache.cassandra.tools.nodetool.nodesync;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "status",
   description = "Return the current status of the NodeSync service (whether it is running on the connected node or not)"
)
public class Status extends NodeTool.NodeToolCmd {
   @Option(
      name = {"-b", "--boolean-output"},
      description = "Simply output 'true' if the service is running and 'false' otherwise; makes the output less human readable but more easily consumed by scripts"
   )
   private boolean booleanOutput = false;

   public Status() {
   }

   public void execute(NodeProbe probe) {
      try {
         boolean isRunning = probe.nodeSyncStatus();
         if(this.booleanOutput) {
            System.out.println(isRunning);
         } else {
            System.out.println(isRunning?"The NodeSync service is running":"The NodeSync service is not running");
         }

      } catch (Exception var3) {
         throw new RuntimeException("Unexpected error while checking the NodeSync service status", var3);
      }
   }
}
