package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "drain",
   description = "Drain the node (stop accepting writes and flush all tables)"
)
public class Drain extends NodeTool.NodeToolCmd {
   public Drain() {
   }

   public void execute(NodeProbe probe) {
      try {
         probe.drain();
      } catch (InterruptedException | ExecutionException | IOException var3) {
         throw new RuntimeException("Error occurred during flushing", var3);
      }
   }
}
