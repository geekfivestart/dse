package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "gettraceprobability",
   description = "Print the current trace probability value"
)
public class GetTraceProbability extends NodeTool.NodeToolCmd {
   public GetTraceProbability() {
   }

   public void execute(NodeProbe probe) {
      System.out.println("Current trace probability: " + probe.getTraceProbability());
   }
}
