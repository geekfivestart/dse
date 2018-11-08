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
   name = "relocatesstables",
   description = "Relocates sstables to the correct disk"
)
public class RelocateSSTables extends NodeTool.NodeToolCmd {
   @Arguments(
      usage = "<keyspace> <table>",
      description = "The keyspace and table name"
   )
   private List<String> args = new ArrayList();
   @Option(
      title = "jobs",
      name = {"-j", "--jobs"},
      description = "Number of sstables to relocate simultanously, set to 0 to use all available compaction threads"
   )
   private int jobs = 2;

   public RelocateSSTables() {
   }

   public void execute(NodeProbe probe) {
      List<String> keyspaces = this.parseOptionalKeyspace(this.args, probe);
      String[] cfnames = this.parseOptionalTables(this.args);

      try {
         Iterator var4 = keyspaces.iterator();

         while(var4.hasNext()) {
            String keyspace = (String)var4.next();
            probe.relocateSSTables(this.jobs, keyspace, cfnames);
         }

      } catch (Exception var6) {
         throw new RuntimeException("Got error while relocating", var6);
      }
   }
}
