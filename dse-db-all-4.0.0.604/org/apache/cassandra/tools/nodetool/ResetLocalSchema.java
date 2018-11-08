package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import java.io.IOException;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "resetlocalschema",
   description = "Reset node's local schema and resync"
)
public class ResetLocalSchema extends NodeTool.NodeToolCmd {
   public ResetLocalSchema() {
   }

   public void execute(NodeProbe probe) {
      try {
         probe.resetLocalSchema();
      } catch (IOException var3) {
         throw new RuntimeException(var3);
      }
   }
}
