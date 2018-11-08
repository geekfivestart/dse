package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "abortrebuild",
   description = "Abort a currently running rebuild operation. Currently active streams will finish but no new streams will be started."
)
public class AbortRebuild extends NodeTool.NodeToolCmd {
   @Option(
      title = "reason",
      name = {"-r", "--reason"},
      description = "Specify a reason to be logged."
   )
   private String reason = null;

   public AbortRebuild() {
   }

   public void execute(NodeProbe probe) {
      probe.abortRebuild(this.reason);
   }
}
