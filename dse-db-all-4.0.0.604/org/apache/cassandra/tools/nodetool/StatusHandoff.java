package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import java.util.Iterator;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "statushandoff",
   description = "Status of storing future hints on the current node"
)
public class StatusHandoff extends NodeTool.NodeToolCmd {
   public StatusHandoff() {
   }

   public void execute(NodeProbe probe) {
      System.out.println(String.format("Hinted handoff is %s", new Object[]{probe.isHandoffEnabled()?"running":"not running"}));
      Iterator var2 = probe.getHintedHandoffDisabledDCs().iterator();

      while(var2.hasNext()) {
         String dc = (String)var2.next();
         System.out.println(String.format("Data center %s is disabled", new Object[]{dc}));
      }

   }
}
