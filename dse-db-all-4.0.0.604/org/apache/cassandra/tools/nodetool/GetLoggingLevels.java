package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import java.util.Iterator;
import java.util.Map.Entry;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "getlogginglevels",
   description = "Get the runtime logging levels"
)
public class GetLoggingLevels extends NodeTool.NodeToolCmd {
   public GetLoggingLevels() {
   }

   public void execute(NodeProbe probe) {
      System.out.printf("%n%-50s%10s%n", new Object[]{"Logger Name", "Log Level"});
      Iterator var2 = probe.getLoggingLevels().entrySet().iterator();

      while(var2.hasNext()) {
         Entry<String, String> entry = (Entry)var2.next();
         System.out.printf("%-50s%10s%n", new Object[]{entry.getKey(), entry.getValue()});
      }

   }
}
