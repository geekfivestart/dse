package org.apache.cassandra.tools.nodetool;

import com.google.common.base.Preconditions;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import java.util.ArrayList;
import java.util.List;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "settimeout",
   description = "Set the specified timeout in ms, or 0 to disable timeout"
)
public class SetTimeout extends NodeTool.NodeToolCmd {
   @Arguments(
      usage = "<timeout_type> <timeout_in_ms>",
      description = "Timeout type followed by value in ms (0 disables socket streaming timeout). Type should be one of (read, range, write, counterwrite, cascontention, truncate, misc (general rpc_timeout_in_ms))",
      required = true
   )
   private List<String> args = new ArrayList();

   public SetTimeout() {
   }

   public void execute(NodeProbe probe) {
      Preconditions.checkArgument(this.args.size() == 2, "Timeout type followed by value in ms (0 disables socket streaming timeout). Type should be one of (read, range, write, counterwrite, cascontention, truncate, misc (general rpc_timeout_in_ms))");

      try {
         String type = (String)this.args.get(0);
         long timeout = Long.parseLong((String)this.args.get(1));
         probe.setTimeout(type, timeout);
      } catch (Exception var5) {
         throw new IllegalArgumentException(var5.getMessage());
      }
   }
}
