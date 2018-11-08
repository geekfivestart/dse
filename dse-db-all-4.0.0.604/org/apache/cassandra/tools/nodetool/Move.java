package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import java.io.IOException;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "move",
   description = "Move node on the token ring to a new token"
)
public class Move extends NodeTool.NodeToolCmd {
   @Arguments(
      usage = "<new token>",
      description = "The new token.",
      required = true
   )
   private String newToken = "";

   public Move() {
   }

   public void execute(NodeProbe probe) {
      try {
         probe.move(this.newToken);
      } catch (IOException var3) {
         throw new RuntimeException("Error during moving node", var3);
      }
   }
}
