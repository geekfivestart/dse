package org.apache.cassandra.tools.nodetool;

import com.google.common.base.Preconditions;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import java.util.ArrayList;
import java.util.List;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "setcachecapacity",
   description = "Set global key, row, and counter cache capacities (in MB units)"
)
public class SetCacheCapacity extends NodeTool.NodeToolCmd {
   @Arguments(
      title = "<key-cache-capacity> <row-cache-capacity> <counter-cache-capacity>",
      usage = "<key-cache-capacity> <row-cache-capacity> <counter-cache-capacity>",
      description = "Key cache, row cache, and counter cache (in MB)",
      required = true
   )
   private List<Integer> args = new ArrayList();

   public SetCacheCapacity() {
   }

   public void execute(NodeProbe probe) {
      Preconditions.checkArgument(this.args.size() == 3, "setcachecapacity requires key-cache-capacity, row-cache-capacity, and counter-cache-capacity args.");
      probe.setCacheCapacities(((Integer)this.args.get(0)).intValue(), ((Integer)this.args.get(1)).intValue(), ((Integer)this.args.get(2)).intValue());
   }
}
