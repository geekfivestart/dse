package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "removenode",
   description = "Show status of current node removal, force completion of pending removal or remove provided ID"
)
public class RemoveNode extends NodeTool.NodeToolCmd {
   @Arguments(
      title = "remove_operation",
      usage = "<status>|<force>|<ID>",
      description = "Show status of current node removal, force completion of pending removal, or remove provided ID",
      required = true
   )
   private String removeOperation = "";

   public RemoveNode() {
   }

   public void execute(NodeProbe probe) {
      String var2 = this.removeOperation;
      byte var3 = -1;
      switch(var2.hashCode()) {
      case -892481550:
         if(var2.equals("status")) {
            var3 = 0;
         }
         break;
      case 97618667:
         if(var2.equals("force")) {
            var3 = 1;
         }
      }

      switch(var3) {
      case 0:
         System.out.println("RemovalStatus: " + probe.getRemovalStatus());
         break;
      case 1:
         System.out.println("RemovalStatus: " + probe.getRemovalStatus());
         probe.forceRemoveCompletion();
         break;
      default:
         probe.removeNode(this.removeOperation);
      }

   }
}
