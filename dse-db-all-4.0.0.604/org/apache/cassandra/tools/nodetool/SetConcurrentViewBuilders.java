package org.apache.cassandra.tools.nodetool;

import com.google.common.base.Preconditions;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "setconcurrentviewbuilders",
   description = "Set the number of concurrent view builders in the system"
)
public class SetConcurrentViewBuilders extends NodeTool.NodeToolCmd {
   @Arguments(
      title = "concurrent_view_builders",
      usage = "<value>",
      description = "Number of concurrent view builders, greater than 0.",
      required = true
   )
   private Integer concurrentViewBuilders = null;

   public SetConcurrentViewBuilders() {
   }

   protected void execute(NodeProbe probe) {
      Preconditions.checkArgument(this.concurrentViewBuilders.intValue() > 0, "concurrent_view_builders should be great than 0.");
      probe.setConcurrentViewBuilders(this.concurrentViewBuilders.intValue());
   }
}
