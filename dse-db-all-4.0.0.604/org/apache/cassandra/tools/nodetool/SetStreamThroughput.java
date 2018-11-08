package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "setstreamthroughput",
   description = "Set the Mb/s throughput cap for streaming in the system, or 0 to disable throttling"
)
public class SetStreamThroughput extends NodeTool.NodeToolCmd {
   @Arguments(
      title = "stream_throughput",
      usage = "<value_in_mb>",
      description = "Value in Mb, 0 to disable throttling",
      required = true
   )
   private Integer streamThroughput = null;

   public SetStreamThroughput() {
   }

   public void execute(NodeProbe probe) {
      probe.setStreamThroughput(this.streamThroughput.intValue());
   }
}
