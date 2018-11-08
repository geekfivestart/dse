package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "enablebackup",
   description = "Enable incremental backup"
)
public class EnableBackup extends NodeTool.NodeToolCmd {
   public EnableBackup() {
   }

   public void execute(NodeProbe probe) {
      probe.setIncrementalBackupsEnabled(true);
   }
}
