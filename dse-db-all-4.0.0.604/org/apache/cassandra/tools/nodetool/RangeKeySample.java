package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "rangekeysample",
   description = "Shows the sampled keys held across all keyspaces"
)
public class RangeKeySample extends NodeTool.NodeToolCmd {
   public RangeKeySample() {
   }

   public void execute(NodeProbe probe) {
      System.out.println("RangeKeySample: ");
      List<String> tokenStrings = probe.sampleKeyRange();
      Iterator var3 = tokenStrings.iterator();

      while(var3.hasNext()) {
         String tokenString = (String)var3.next();
         System.out.println("\t" + tokenString);
      }

   }
}
