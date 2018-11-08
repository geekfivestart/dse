package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "getconcurrentcompactors",
   description = "Get the number of concurrent compactors in the system."
)
public class GetConcurrentCompactors extends NodeTool.NodeToolCmd {
   public GetConcurrentCompactors() {
   }

   protected void execute(NodeProbe probe) {
      System.out.println("Current concurrent compactors in the system is: \n" + probe.getConcurrentCompactors());
   }
}
