package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "setmaxhintwindow",
   description = "Set the specified max hint window in ms"
)
public class SetMaxHintWindow extends NodeTool.NodeToolCmd {
   @Arguments(
      title = "max_hint_window",
      usage = "<value_in_ms>",
      description = "Value of maxhintwindow in ms",
      required = true
   )
   private Integer maxHintWindow = null;

   public SetMaxHintWindow() {
   }

   public void execute(NodeProbe probe) {
      probe.setMaxHintWindow(this.maxHintWindow.intValue());
   }
}
