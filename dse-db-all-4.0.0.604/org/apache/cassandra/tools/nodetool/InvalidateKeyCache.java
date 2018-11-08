package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "invalidatekeycache",
   description = "Invalidate the key cache"
)
public class InvalidateKeyCache extends NodeTool.NodeToolCmd {
   public InvalidateKeyCache() {
   }

   public void execute(NodeProbe probe) {
      probe.invalidateKeyCache();
   }
}
