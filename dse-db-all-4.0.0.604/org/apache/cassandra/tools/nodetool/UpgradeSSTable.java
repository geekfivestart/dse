package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "upgradesstables",
   description = "Rewrite sstables (for the requested tables) that are not on the current version (thus upgrading them to said current version)"
)
public class UpgradeSSTable extends NodeTool.NodeToolCmd {
   @Arguments(
      usage = "[<keyspace> <tables>...]",
      description = "The keyspace followed by one or many tables"
   )
   private List<String> args = new ArrayList();
   @Option(
      title = "include_all",
      name = {"-a", "--include-all-sstables"},
      description = "Use -a to include all sstables, even those already on the current version"
   )
   private boolean includeAll = false;
   @Option(
      title = "jobs",
      name = {"-j", "--jobs"},
      description = "Number of sstables to upgrade simultanously, set to 0 to use all available compaction threads"
   )
   private int jobs = 2;

   public UpgradeSSTable() {
   }

   public void execute(NodeProbe probe) {
      List<String> keyspaces = this.parseOptionalKeyspace(this.args, probe);
      String[] tableNames = this.parseOptionalTables(this.args);
      Iterator var4 = keyspaces.iterator();

      while(var4.hasNext()) {
         String keyspace = (String)var4.next();

         try {
            probe.upgradeSSTables(System.out, keyspace, !this.includeAll, this.jobs, tableNames);
         } catch (Exception var7) {
            throw new RuntimeException("Error occurred during enabling auto-compaction", var7);
         }
      }

   }
}
