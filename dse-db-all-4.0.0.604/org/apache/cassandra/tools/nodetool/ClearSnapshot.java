package org.apache.cassandra.tools.nodetool;

import com.google.common.collect.Iterables;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.commons.lang3.StringUtils;

@Command(
   name = "clearsnapshot",
   description = "Remove the snapshot with the given name from the given keyspaces. If no snapshotName is specified we will remove all snapshots"
)
public class ClearSnapshot extends NodeTool.NodeToolCmd {
   @Arguments(
      usage = "[<keyspaces>...] ",
      description = "Remove snapshots from the given keyspaces"
   )
   private List<String> keyspaces = new ArrayList();
   @Option(
      title = "snapshot_name",
      name = {"-t"},
      description = "Remove the snapshot with a given name"
   )
   private String snapshotName = "";
   @Option(
      title = "clear_all_snapshots",
      name = {"--all"},
      description = "Removes all snapshots"
   )
   private boolean clearAllSnapshots = false;

   public ClearSnapshot() {
   }

   public void execute(NodeProbe probe) {
      if(this.snapshotName.isEmpty() && !this.clearAllSnapshots) {
         throw new RuntimeException("Specify snapshot name or --all");
      } else if(!this.snapshotName.isEmpty() && this.clearAllSnapshots) {
         throw new RuntimeException("Specify only one of snapshot name or --all");
      } else {
         StringBuilder sb = new StringBuilder();
         sb.append("Requested clearing snapshot(s) for ");
         if(this.keyspaces.isEmpty()) {
            sb.append("[all keyspaces]");
         } else {
            sb.append("[").append(StringUtils.join(this.keyspaces, ", ")).append("]");
         }

         if(this.snapshotName.isEmpty()) {
            sb.append(" with [all snapshots]");
         } else {
            sb.append(" with snapshot name [").append(this.snapshotName).append("]");
         }

         System.out.println(sb.toString());

         try {
            probe.clearSnapshot(this.snapshotName, (String[])Iterables.toArray(this.keyspaces, String.class));
         } catch (IOException var4) {
            throw new RuntimeException("Error during clearing snapshots", var4);
         }
      }
   }
}
