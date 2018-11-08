package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import java.io.IOError;
import java.io.IOException;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "replaybatchlog",
   description = "Kick off batchlog replay and wait for finish"
)
public class ReplayBatchlog extends NodeTool.NodeToolCmd {
   public ReplayBatchlog() {
   }

   protected void execute(NodeProbe probe) {
      try {
         probe.replayBatchlog();
      } catch (IOException var3) {
         throw new IOError(var3);
      }
   }
}
