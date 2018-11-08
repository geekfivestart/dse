package org.apache.cassandra.tools.nodetool;

import com.google.common.base.Preconditions;
import io.airlift.airline.Command;
import java.io.IOException;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "join",
   description = "Join the ring"
)
public class Join extends NodeTool.NodeToolCmd {
   public Join() {
   }

   public void execute(NodeProbe probe) {
      Preconditions.checkState(!probe.isJoined(), "This node has already joined the ring.");

      try {
         probe.joinRing();
      } catch (IOException var3) {
         throw new RuntimeException("Error during joining the ring", var3);
      }
   }
}
