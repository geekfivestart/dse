package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "getstreamthroughput",
   description = "Print the Mb/s throughput cap for streaming in the system"
)
public class GetStreamThroughput extends NodeTool.NodeToolCmd {
   public GetStreamThroughput() {
   }

   public void execute(NodeProbe probe) {
      System.out.println("Current stream throughput: " + probe.getStreamThroughput() + " Mb/s");
      System.out.println("Current streaming connections per host: " + probe.getStreamingConnectionsPerHost());
   }
}
