package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "invalidatecountercache",
   description = "Invalidate the counter cache"
)
public class InvalidateCounterCache extends NodeTool.NodeToolCmd {
   public InvalidateCounterCache() {
   }

   public void execute(NodeProbe probe) {
      probe.invalidateCounterCache();
   }
}
