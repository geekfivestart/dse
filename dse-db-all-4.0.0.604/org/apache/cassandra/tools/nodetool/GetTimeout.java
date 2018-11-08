package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "gettimeout",
   description = "Print the timeout of the given type in ms"
)
public class GetTimeout extends NodeTool.NodeToolCmd {
   public static final String TIMEOUT_TYPES = "read, range, write, counterwrite, cascontention, truncate, misc (general rpc_timeout_in_ms)";
   @Arguments(
      usage = "<timeout_type>",
      description = "The timeout type, one or more of (read, range, write, counterwrite, cascontention, truncate, misc (general rpc_timeout_in_ms))"
   )
   private List<String> args = new ArrayList();

   public GetTimeout() {
   }

   public void execute(NodeProbe probe) {
      if(this.args.isEmpty()) {
         this.args.addAll(Arrays.asList(new String[]{"read", "range", "write", "counterwrite", "cascontention", "truncate", "misc"}));
      }

      try {
         Iterator var2 = this.args.iterator();

         while(var2.hasNext()) {
            String type = (String)var2.next();
            System.out.println("Current timeout for type " + type + ": " + probe.getTimeout(type) + " ms");
         }

      } catch (Exception var4) {
         throw new IllegalArgumentException(var4.getMessage());
      }
   }
}
