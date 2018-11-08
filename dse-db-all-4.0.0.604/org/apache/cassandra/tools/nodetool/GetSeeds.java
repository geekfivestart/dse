package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import java.util.List;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "getseeds",
   description = "Get the currently in use seed node IP list excluding the node IP"
)
public class GetSeeds extends NodeTool.NodeToolCmd {
   public GetSeeds() {
   }

   public void execute(NodeProbe probe) {
      List<String> seedList = probe.getSeeds();
      if(seedList.isEmpty()) {
         System.out.println("Seed node list does not contain any remote node IPs");
      } else {
         System.out.println("Current list of seed node IPs excluding the current node IP: " + String.join(" ", seedList));
      }

   }
}
