package org.apache.cassandra.tools.nodetool;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import java.util.ArrayList;
import java.util.List;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "rebuild_index",
   description = "A full rebuild of native secondary indexes for a given table"
)
public class RebuildIndex extends NodeTool.NodeToolCmd {
   @Arguments(
      usage = "<keyspace> <table> <indexName...>",
      description = "The keyspace and table name followed by a list of index names"
   )
   List<String> args = new ArrayList();

   public RebuildIndex() {
   }

   public void execute(NodeProbe probe) {
      Preconditions.checkArgument(this.args.size() >= 3, "rebuild_index requires ks, cf and idx args");
      probe.rebuildIndex((String)this.args.get(0), (String)this.args.get(1), (String[])Iterables.toArray(this.args.subList(2, this.args.size()), String.class));
   }
}
