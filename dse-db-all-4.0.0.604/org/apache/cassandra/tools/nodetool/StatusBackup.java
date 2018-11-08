package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "statusbackup",
   description = "Status of incremental backup"
)
public class StatusBackup extends NodeTool.NodeToolCmd {
   public StatusBackup() {
   }

   public void execute(NodeProbe probe) {
      System.out.println(probe.isIncrementalBackupsEnabled()?"running":"not running");
   }
}
