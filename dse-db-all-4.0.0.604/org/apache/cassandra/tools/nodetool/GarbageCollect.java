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
   name = "garbagecollect",
   description = "Remove deleted data from one or more tables"
)
public class GarbageCollect extends NodeTool.NodeToolCmd {
   @Arguments(
      usage = "[<keyspace> <tables>...]",
      description = "The keyspace followed by one or many tables"
   )
   private List<String> args = new ArrayList();
   @Option(
      title = "granularity",
      name = {"-g", "--granularity"},
      allowedValues = {"ROW", "CELL"},
      description = "Granularity of garbage removal. ROW (default) removes deleted partitions and rows, CELL also removes overwritten or deleted cells."
   )
   private String tombstoneOption = "ROW";
   @Option(
      title = "jobs",
      name = {"-j", "--jobs"},
      description = "Number of sstables to cleanup simultanously, set to 0 to use all available compaction threads"
   )
   private int jobs = 2;

   public GarbageCollect() {
   }

   public void execute(NodeProbe probe) {
      List<String> keyspaces = this.parseOptionalKeyspace(this.args, probe);
      String[] tableNames = this.parseOptionalTables(this.args);
      Iterator var4 = keyspaces.iterator();

      while(var4.hasNext()) {
         String keyspace = (String)var4.next();

         try {
            probe.garbageCollect(System.out, this.tombstoneOption, this.jobs, keyspace, tableNames);
         } catch (Exception var7) {
            throw new RuntimeException("Error occurred during garbage collection", var7);
         }
      }

   }
}
