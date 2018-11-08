package org.apache.cassandra.tools.nodetool.nodesync;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "enable",
   description = "Enable the NodeSync service"
)
public class Enable extends NodeTool.NodeToolCmd {
   @Option(
      name = {"-t", "--timeout"},
      description = "Timeout (in seconds) for waiting on the service to finish startup (default: 2 minutes)"
   )
   private long timeoutSec;

   public Enable() {
      this.timeoutSec = TimeUnit.MINUTES.toSeconds(2L);
   }

   public void execute(NodeProbe probe) {
      try {
         if(!probe.enableNodeSync(this.timeoutSec, TimeUnit.SECONDS)) {
            System.out.println("The NodeSync service is already running");
         }
      } catch (TimeoutException var3) {
         System.err.println("Error: timed-out waiting for the NodeSync service to start (timeout was " + this.timeoutSec + " seconds).");
         System.exit(1);
      } catch (Exception var4) {
         throw new RuntimeException("Unexpected error enabling the NodeSync service", var4);
      }

   }
}
