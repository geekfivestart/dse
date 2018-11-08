package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "getcompactionthroughput",
   description = "Print the MB/s throughput cap for compaction in the system"
)
public class GetCompactionThroughput extends NodeTool.NodeToolCmd {
   public GetCompactionThroughput() {
   }

   public void execute(NodeProbe probe) {
      System.out.println("Current compaction throughput: " + probe.getCompactionThroughput() + " MB/s");
   }
}
