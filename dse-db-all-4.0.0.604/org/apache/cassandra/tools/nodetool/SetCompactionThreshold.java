package org.apache.cassandra.tools.nodetool;

import com.google.common.base.Preconditions;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import java.util.ArrayList;
import java.util.List;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "setcompactionthreshold",
   description = "Set min and max compaction thresholds for a given table"
)
public class SetCompactionThreshold extends NodeTool.NodeToolCmd {
   @Arguments(
      title = "<keyspace> <table> <minthreshold> <maxthreshold>",
      usage = "<keyspace> <table> <minthreshold> <maxthreshold>",
      description = "The keyspace, the table, min and max threshold",
      required = true
   )
   private List<String> args = new ArrayList();

   public SetCompactionThreshold() {
   }

   public void execute(NodeProbe probe) {
      Preconditions.checkArgument(this.args.size() == 4, "setcompactionthreshold requires ks, cf, min, and max threshold args.");
      int minthreshold = Integer.parseInt((String)this.args.get(2));
      int maxthreshold = Integer.parseInt((String)this.args.get(3));
      Preconditions.checkArgument(minthreshold >= 0 && maxthreshold >= 0, "Thresholds must be positive integers");
      Preconditions.checkArgument(minthreshold <= maxthreshold, "Min threshold cannot be greater than max.");
      Preconditions.checkArgument(minthreshold >= 2 || maxthreshold == 0, "Min threshold must be at least 2");
      probe.setCompactionThreshold((String)this.args.get(0), (String)this.args.get(1), minthreshold, maxthreshold);
   }
}
