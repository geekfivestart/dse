package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "getconcurrentviewbuilders",
   description = "Get the number of concurrent view builders in the system"
)
public class GetConcurrentViewBuilders extends NodeTool.NodeToolCmd {
   public GetConcurrentViewBuilders() {
   }

   protected void execute(NodeProbe probe) {
      System.out.println("Current number of concurrent view builders in the system is: \n" + probe.getConcurrentViewBuilders());
   }
}
