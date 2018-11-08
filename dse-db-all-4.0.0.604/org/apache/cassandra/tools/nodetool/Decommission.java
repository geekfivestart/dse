package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "decommission",
   description = "Decommission the *node I am connecting to*"
)
public class Decommission extends NodeTool.NodeToolCmd {
   @Option(
      title = "force",
      name = {"-f", "--force"},
      description = "Force decommission of this node even when it reduces the number of replicas to below configured RF"
   )
   private boolean force = false;

   public Decommission() {
   }

   public void execute(NodeProbe probe) {
      try {
         probe.decommission(this.force);
      } catch (InterruptedException var3) {
         throw new RuntimeException("Error decommissioning node", var3);
      } catch (UnsupportedOperationException var4) {
         throw new IllegalStateException("Unsupported operation: " + var4.getMessage(), var4);
      }
   }
}
