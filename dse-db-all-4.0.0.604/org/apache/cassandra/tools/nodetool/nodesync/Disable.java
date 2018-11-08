package org.apache.cassandra.tools.nodetool.nodesync;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "disable",
   description = "Disable the NodeSync service"
)
public class Disable extends NodeTool.NodeToolCmd {
   @Option(
      name = {"-f", "--force"},
      description = "Triggers a forced shutdown that don't let already started segment validations finished"
   )
   private boolean force = false;
   @Option(
      name = {"-t", "--timeout"},
      description = "Timeout (in seconds) for waiting on the service to finish shutdown (default: 2 minutes)"
   )
   private long timeoutSec;

   public Disable() {
      this.timeoutSec = TimeUnit.MINUTES.toSeconds(2L);
   }

   public void execute(NodeProbe probe) {
      try {
         if(!probe.disableNodeSync(this.force, this.timeoutSec, TimeUnit.SECONDS)) {
            System.out.println("The NodeSync service is not running");
         }
      } catch (TimeoutException var3) {
         System.err.println("Error: timed-out waiting for the NodeSync service to stop (timeout was " + this.timeoutSec + " seconds).");
         System.exit(1);
      } catch (Exception var4) {
         throw new RuntimeException("Unexpected error disabling the NodeSync service", var4);
      }

   }
}
