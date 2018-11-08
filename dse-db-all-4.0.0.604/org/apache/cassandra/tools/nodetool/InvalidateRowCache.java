package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "invalidaterowcache",
   description = "Invalidate the row cache"
)
public class InvalidateRowCache extends NodeTool.NodeToolCmd {
   public InvalidateRowCache() {
   }

   public void execute(NodeProbe probe) {
      probe.invalidateRowCache();
   }
}
