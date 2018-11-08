package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "resumehandoff",
   description = "Resume hints delivery process"
)
public class ResumeHandoff extends NodeTool.NodeToolCmd {
   public ResumeHandoff() {
   }

   public void execute(NodeProbe probe) {
      probe.resumeHintsDelivery();
   }
}
