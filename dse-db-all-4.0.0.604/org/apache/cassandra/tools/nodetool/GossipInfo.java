package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "gossipinfo",
   description = "Shows the gossip information for the cluster"
)
public class GossipInfo extends NodeTool.NodeToolCmd {
   public GossipInfo() {
   }

   public void execute(NodeProbe probe) {
      System.out.println(probe.getGossipInfo());
   }
}
