package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "truncatehints",
   description = "Truncate all hints on the local node, or truncate hints for the endpoint(s) specified."
)
public class TruncateHints extends NodeTool.NodeToolCmd {
   @Arguments(
      usage = "[endpoint ... ]",
      description = "Endpoint address(es) to delete hints for, either ip address (\"127.0.0.1\") or hostname"
   )
   private String endpoint = "";

   public TruncateHints() {
   }

   public void execute(NodeProbe probe) {
      if(this.endpoint.isEmpty()) {
         probe.truncateHints();
      } else {
         probe.truncateHints(this.endpoint);
      }

   }
}
