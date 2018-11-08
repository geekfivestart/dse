package org.apache.cassandra.tools.nodetool;

import com.google.common.base.Preconditions;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import java.util.ArrayList;
import java.util.List;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "enablehintsfordc",
   description = "Enable hints for a data center that was previsouly disabled"
)
public class EnableHintsForDC extends NodeTool.NodeToolCmd {
   @Arguments(
      usage = "<datacenter>",
      description = "The data center to enable"
   )
   private List<String> args = new ArrayList();

   public EnableHintsForDC() {
   }

   public void execute(NodeProbe probe) {
      Preconditions.checkArgument(this.args.size() == 1, "enablehintsfordc requires exactly one data center");
      probe.enableHintsForDC((String)this.args.get(0));
   }
}
