package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "disablebackup",
   description = "Disable incremental backup"
)
public class DisableBackup extends NodeTool.NodeToolCmd {
   public DisableBackup() {
   }

   public void execute(NodeProbe probe) {
      probe.setIncrementalBackupsEnabled(false);
   }
}
