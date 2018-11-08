package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

@Command(
   name = "statusautocompaction",
   description = "status of autocompaction of the given keyspace and table"
)
public class StatusAutoCompaction extends NodeTool.NodeToolCmd {
   @Arguments(
      usage = "[<keyspace> <tables>...]",
      description = "The keyspace followed by one or many tables"
   )
   private List<String> args = new ArrayList();
   @Option(
      title = "show_all",
      name = {"-a", "--all"},
      description = "Show auto compaction status for each keyspace/table"
   )
   private boolean showAll = false;

   public StatusAutoCompaction() {
   }

   public void execute(NodeProbe probe) {
      List<String> keyspaces = this.parseOptionalKeyspace(this.args, probe);
      String[] tableNames = this.parseOptionalTables(this.args);
      boolean allDisabled = true;
      boolean allEnabled = true;
      TableBuilder table = new TableBuilder();
      table.add(new String[]{"Keyspace", "Table", "Status"});

      try {
         Iterator var7 = keyspaces.iterator();

         while(var7.hasNext()) {
            String keyspace = (String)var7.next();
            Map<String, Boolean> statuses = probe.getAutoCompactionDisabled(keyspace, tableNames);
            Iterator var10 = statuses.entrySet().iterator();

            while(var10.hasNext()) {
               Entry<String, Boolean> status = (Entry)var10.next();
               String tableName = (String)status.getKey();
               boolean disabled = ((Boolean)status.getValue()).booleanValue();
               allDisabled &= disabled;
               allEnabled &= !disabled;
               table.add(new String[]{keyspace, tableName, !disabled?"running":"not running"});
            }
         }

         if(this.showAll) {
            table.printTo(System.out);
         } else {
            System.out.println(allEnabled?"running":(allDisabled?"not running":"partially running"));
         }

      } catch (IOException var14) {
         throw new RuntimeException("Error occurred during status-auto-compaction", var14);
      }
   }
}
