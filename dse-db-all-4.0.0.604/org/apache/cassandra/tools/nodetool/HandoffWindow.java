package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "handoffwindow",
   description = "Print current hinted handoff window"
)
public class HandoffWindow extends NodeTool.NodeToolCmd {
   public HandoffWindow() {
   }

   public void execute(NodeProbe probe) {
      System.out.println(String.format("Hinted handoff window is %s", new Object[]{Integer.valueOf(probe.getMaxHintWindow())}));
   }
}
