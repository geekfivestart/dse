package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import java.io.IOError;
import java.io.IOException;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "resume",
   description = "Resume bootstrap streaming"
)
public class BootstrapResume extends NodeTool.NodeToolCmd {
   public BootstrapResume() {
   }

   protected void execute(NodeProbe probe) {
      try {
         probe.resumeBootstrap(System.out);
      } catch (IOException var3) {
         throw new IOError(var3);
      }
   }
}
