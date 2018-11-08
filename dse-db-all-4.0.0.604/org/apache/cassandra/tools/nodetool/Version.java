package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "version",
   description = "Print DSE DB version"
)
public class Version extends NodeTool.NodeToolCmd {
   public Version() {
   }

   public void execute(NodeProbe probe) {
      System.out.println("ReleaseVersion: " + probe.getReleaseVersion());
   }
}
