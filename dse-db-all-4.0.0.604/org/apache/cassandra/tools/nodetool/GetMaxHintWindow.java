package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "getmaxhintwindow",
   description = "Print the max hint window in ms"
)
public class GetMaxHintWindow extends NodeTool.NodeToolCmd {
   public GetMaxHintWindow() {
   }

   public void execute(NodeProbe probe) {
      System.out.println("Current max hint window: " + probe.getMaxHintWindow() + " ms");
   }
}
