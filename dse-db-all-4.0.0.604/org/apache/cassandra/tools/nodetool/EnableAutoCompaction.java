package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "enableautocompaction",
   description = "Enable autocompaction for the given keyspace and table"
)
public class EnableAutoCompaction extends NodeTool.NodeToolCmd {
   @Arguments(
      usage = "[<keyspace> <tables>...]",
      description = "The keyspace followed by one or many tables"
   )
   private List<String> args = new ArrayList();

   public EnableAutoCompaction() {
   }

   public void execute(NodeProbe probe) {
      List<String> keyspaces = this.parseOptionalKeyspace(this.args, probe);
      String[] tableNames = this.parseOptionalTables(this.args);
      Iterator var4 = keyspaces.iterator();

      while(var4.hasNext()) {
         String keyspace = (String)var4.next();

         try {
            probe.enableAutoCompaction(keyspace, tableNames);
         } catch (IOException var7) {
            throw new RuntimeException("Error occurred during enabling auto-compaction", var7);
         }
      }

   }
}
