package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "cleanup",
   description = "Triggers the immediate cleanup of keys no longer belonging to a node. By default, clean all keyspaces"
)
public class Cleanup extends NodeTool.NodeToolCmd {
   @Arguments(
      usage = "[<keyspace> <tables>...]",
      description = "The keyspace followed by one or many tables"
   )
   private List<String> args = new ArrayList();
   @Option(
      title = "jobs",
      name = {"-j", "--jobs"},
      description = "Number of sstables to cleanup simultanously, set to 0 to use all available compaction threads"
   )
   private int jobs = 2;

   public Cleanup() {
   }

   public void execute(NodeProbe probe) {
      List<String> keyspaces = this.parseOptionalKeyspace(this.args, probe, NodeTool.NodeToolCmd.KeyspaceSet.NON_LOCAL_STRATEGY);
      String[] tableNames = this.parseOptionalTables(this.args);
      Iterator var4 = keyspaces.iterator();

      while(var4.hasNext()) {
         String keyspace = (String)var4.next();
         if(!SchemaConstants.isLocalSystemKeyspace(keyspace)) {
            try {
               probe.forceKeyspaceCleanup(System.out, this.jobs, keyspace, tableNames);
            } catch (Exception var7) {
               throw new RuntimeException("Error occurred during cleanup", var7);
            }
         }
      }

   }
}
