package org.apache.cassandra.tools.nodetool;

import com.google.common.collect.Iterables;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.commons.lang3.StringUtils;

@Command(
   name = "snapshot",
   description = "Take a snapshot of specified keyspaces or a snapshot of the specified table"
)
public class Snapshot extends NodeTool.NodeToolCmd {
   @Arguments(
      usage = "[<keyspaces...>]",
      description = "List of keyspaces. By default, all keyspaces"
   )
   private List<String> keyspaces = new ArrayList();
   @Option(
      title = "table",
      name = {"-cf", "--column-family", "--table"},
      description = "The table name (you must specify one and only one keyspace for using this option)"
   )
   private String table = null;
   @Option(
      title = "tag",
      name = {"-t", "--tag"},
      description = "The name of the snapshot"
   )
   private String snapshotName = Long.toString(System.currentTimeMillis());
   @Option(
      title = "ktlist",
      name = {"-kt", "--kt-list", "-kc", "--kc.list"},
      description = "The list of Keyspace.table to take snapshot.(you must not specify only keyspace)"
   )
   private String ktList = null;
   @Option(
      title = "skip-flush",
      name = {"-sf", "--skip-flush"},
      description = "Do not flush memtables before snapshotting (snapshot will not contain unflushed data)"
   )
   private boolean skipFlush = false;

   public Snapshot() {
   }

   public void execute(NodeProbe probe) {
      try {
         StringBuilder sb = new StringBuilder();
         sb.append("Requested creating snapshot(s) for ");
         Map<String, String> options = new HashMap();
         options.put("skipFlush", Boolean.toString(this.skipFlush));
         if(null != this.ktList && !this.ktList.isEmpty()) {
            this.ktList = this.ktList.replace(" ", "");
            if(!this.keyspaces.isEmpty() || null != this.table) {
               throw new IOException("When specifying the Keyspace columfamily list for a snapshot, you should not specify columnfamily");
            }

            sb.append("[").append(this.ktList).append("]");
            if(!this.snapshotName.isEmpty()) {
               sb.append(" with snapshot name [").append(this.snapshotName).append("]");
            }

            sb.append(" and options ").append(options.toString());
            System.out.println(sb.toString());
            probe.takeMultipleTableSnapshot(this.snapshotName, options, this.ktList.split(","));
            System.out.println("Snapshot directory: " + this.snapshotName);
         } else {
            if(this.keyspaces.isEmpty()) {
               sb.append("[all keyspaces]");
            } else {
               sb.append("[").append(StringUtils.join(this.keyspaces, ", ")).append("]");
            }

            if(!this.snapshotName.isEmpty()) {
               sb.append(" with snapshot name [").append(this.snapshotName).append("]");
            }

            sb.append(" and options ").append(options.toString());
            System.out.println(sb.toString());
            probe.takeSnapshot(this.snapshotName, this.table, options, (String[])Iterables.toArray(this.keyspaces, String.class));
            System.out.println("Snapshot directory: " + this.snapshotName);
         }

      } catch (IOException var4) {
         throw new RuntimeException("Error during taking a snapshot", var4);
      }
   }
}
