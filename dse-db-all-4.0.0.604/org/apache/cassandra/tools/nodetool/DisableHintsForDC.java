package org.apache.cassandra.tools.nodetool;

import com.google.common.base.Preconditions;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import java.util.ArrayList;
import java.util.List;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "disablehintsfordc",
   description = "Disable hints for a data center"
)
public class DisableHintsForDC extends NodeTool.NodeToolCmd {
   @Arguments(
      usage = "<datacenter>",
      description = "The data center to disable"
   )
   private List<String> args = new ArrayList();

   public DisableHintsForDC() {
   }

   public void execute(NodeProbe probe) {
      Preconditions.checkArgument(this.args.size() == 1, "disablehintsfordc requires exactly one data center");
      probe.disableHintsForDC((String)this.args.get(0));
   }
}
