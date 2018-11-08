package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import java.net.UnknownHostException;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "assassinate",
   description = "Forcefully remove a dead node without re-replicating any data.  Use as a last resort if you cannot removenode"
)
public class Assassinate extends NodeTool.NodeToolCmd {
   @Arguments(
      title = "ip address",
      usage = "<ip_address>",
      description = "IP address of the endpoint to assassinate",
      required = true
   )
   private String endpoint = "";

   public Assassinate() {
   }

   public void execute(NodeProbe probe) {
      try {
         probe.assassinateEndpoint(this.endpoint);
      } catch (UnknownHostException var3) {
         throw new RuntimeException(var3);
      }
   }
}
