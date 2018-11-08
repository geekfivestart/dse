package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "getinterdcstreamthroughput",
   description = "Print the Mb/s throughput cap for inter-datacenter streaming in the system"
)
public class GetInterDCStreamThroughput extends NodeTool.NodeToolCmd {
   public GetInterDCStreamThroughput() {
   }

   public void execute(NodeProbe probe) {
      System.out.println("Current inter-datacenter stream throughput: " + probe.getInterDCStreamThroughput() + " Mb/s");
   }
}
