package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "statusgossip",
   description = "Status of gossip"
)
public class StatusGossip extends NodeTool.NodeToolCmd {
   public StatusGossip() {
   }

   public void execute(NodeProbe probe) {
      System.out.println(probe.isGossipRunning()?"running":"not running");
   }
}
