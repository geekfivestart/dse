package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import java.util.List;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "reloadseeds",
   description = "Reload the seed node list from the seed node provider"
)
public class ReloadSeeds extends NodeTool.NodeToolCmd {
   public ReloadSeeds() {
   }

   public void execute(NodeProbe probe) {
      List<String> seedList = probe.reloadSeeds();
      if(seedList == null) {
         System.out.println("Failed to reload the seed node list.");
      } else if(seedList.isEmpty()) {
         System.out.println("Seed node list does not contain any remote node IPs");
      } else {
         System.out.println("Updated seed node IP list excluding the current node IP: " + String.join(" ", seedList));
      }

   }
}
