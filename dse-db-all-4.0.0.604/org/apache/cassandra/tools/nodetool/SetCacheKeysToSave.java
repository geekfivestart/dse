package org.apache.cassandra.tools.nodetool;

import com.google.common.base.Preconditions;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import java.util.ArrayList;
import java.util.List;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "setcachekeystosave",
   description = "Set number of keys saved by each cache for faster post-restart warmup. 0 to disable"
)
public class SetCacheKeysToSave extends NodeTool.NodeToolCmd {
   @Arguments(
      title = "<key-cache-keys-to-save> <row-cache-keys-to-save> <counter-cache-keys-to-save>",
      usage = "<key-cache-keys-to-save> <row-cache-keys-to-save> <counter-cache-keys-to-save>",
      description = "The number of keys saved by each cache. 0 to disable",
      required = true
   )
   private List<Integer> args = new ArrayList();

   public SetCacheKeysToSave() {
   }

   public void execute(NodeProbe probe) {
      Preconditions.checkArgument(this.args.size() == 3, "setcachekeystosave requires key-cache-keys-to-save, row-cache-keys-to-save, and counter-cache-keys-to-save args.");
      probe.setCacheKeysToSave(((Integer)this.args.get(0)).intValue(), ((Integer)this.args.get(1)).intValue(), ((Integer)this.args.get(2)).intValue());
   }
}
