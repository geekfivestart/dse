package org.apache.cassandra.tools.nodetool.nodesync;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "getrate",
   description = "Retrieve the current synchronization rate limit"
)
public class GetRate extends NodeTool.NodeToolCmd {
   public GetRate() {
   }

   public void execute(NodeProbe probe) {
      System.out.println(String.format("Current rate limit=%d KB/s", new Object[]{Integer.valueOf(probe.getNodeSyncRate())}));
   }
}
