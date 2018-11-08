package org.apache.cassandra.tools.nodetool;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import java.io.IOError;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "mark_unrepaired",
   description = "Mark all SSTables of a table or keyspace as unrepaired. Use when no longer running incremental repair on a table or keyspace."
)
public class MarkUnrepaired extends NodeTool.NodeToolCmd {
   @Arguments(
      usage = "<keyspace> [<tables>...]",
      description = "The keyspace followed by zero or many tables"
   )
   private List<String> args = new ArrayList();
   @Option(
      title = "force",
      name = {"-force", "--force"},
      description = "Use -force to confirm the operation"
   )
   private boolean force = false;

   public MarkUnrepaired() {
   }

   protected void execute(NodeProbe probe) {
      Preconditions.checkArgument(this.args.size() >= 1, "mark_unrepaired requires at least one keyspace as argument");
      List<String> keyspaces = this.parseOptionalKeyspace(this.args, probe, NodeTool.NodeToolCmd.KeyspaceSet.ALL);

      assert keyspaces.size() == 1;

      String[] tables = this.parseOptionalTables(this.args);
      String keyspace = (String)keyspaces.iterator().next();
      if(!this.force) {
         String keyspaceOrTables = tables.length == 0?String.format("keyspace %s", new Object[]{keyspace}):String.format("table(s) %s.[%s]", new Object[]{keyspace, Joiner.on(",").join(tables)});
         throw new IllegalArgumentException(String.format("WARNING: This operation will mark all SSTables of %s as unrepaired, potentially creating new compaction tasks. Only use this when no longer running incremental repair on this node. Use --force option to confirm.", new Object[]{keyspaceOrTables}));
      } else {
         try {
            probe.markAllSSTablesAsUnrepaired(System.out, keyspace, tables);
         } catch (IOException var6) {
            throw new IOError(var6);
         }
      }
   }
}
