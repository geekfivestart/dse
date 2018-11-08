package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "stop",
   description = "Stop compaction"
)
public class Stop extends NodeTool.NodeToolCmd {
   @Arguments(
      title = "compaction_type",
      usage = "<compaction type>",
      description = "Supported types are COMPACTION, VALIDATION, CLEANUP, SCRUB, VERIFY, INDEX_BUILD",
      required = false
   )
   private OperationType compactionType;
   @Option(
      title = "compactionId",
      name = {"-id", "--compaction-id"},
      description = "Use -id to stop a compaction by the specified id. Ids can be found in the transaction log files whose name starts with compaction_, located in the table transactions folder.",
      required = false
   )
   private String compactionId;

   public Stop() {
      this.compactionType = OperationType.UNKNOWN;
      this.compactionId = "";
   }

   public void execute(NodeProbe probe) {
      if(!this.compactionId.isEmpty()) {
         probe.stopById(this.compactionId);
      } else {
         probe.stop(this.compactionType.name());
      }

   }
}
