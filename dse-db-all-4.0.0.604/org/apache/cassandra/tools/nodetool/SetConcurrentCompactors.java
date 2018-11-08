package org.apache.cassandra.tools.nodetool;

import com.google.common.base.Preconditions;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "setconcurrentcompactors",
   description = "Set number of concurrent compactors in the system."
)
public class SetConcurrentCompactors extends NodeTool.NodeToolCmd {
   @Arguments(
      title = "concurrent_compactors",
      usage = "<value>",
      description = "Number of concurrent compactors, greater than 0.",
      required = true
   )
   private Integer concurrentCompactors = null;

   public SetConcurrentCompactors() {
   }

   protected void execute(NodeProbe probe) {
      Preconditions.checkArgument(this.concurrentCompactors.intValue() > 0, "concurrent_compactors should be great than 0.");
      probe.setConcurrentCompactors(this.concurrentCompactors.intValue());
   }
}
