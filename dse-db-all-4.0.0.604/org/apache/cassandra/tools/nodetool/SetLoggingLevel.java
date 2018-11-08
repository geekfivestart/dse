package org.apache.cassandra.tools.nodetool;

import com.google.common.collect.Lists;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "setlogginglevel",
   description = "Set the log level threshold for a given component or class. Will reset to the initial configuration if called with no parameters."
)
public class SetLoggingLevel extends NodeTool.NodeToolCmd {
   @Arguments(
      usage = "<component|class> <level>",
      description = "The component or class to change the level for and the log level threshold to set. Will reset to initial level if omitted. Available components:  bootstrap, compaction, repair, streaming, cql, ring"
   )
   private List<String> args = new ArrayList();

   public SetLoggingLevel() {
   }

   public void execute(NodeProbe probe) {
      String target = this.args.size() >= 1?(String)this.args.get(0):"";
      String level = this.args.size() == 2?(String)this.args.get(1):"";
      List<String> classQualifiers = Collections.singletonList(target);
      if(target.equals("bootstrap")) {
         classQualifiers = Lists.newArrayList(new String[]{"org.apache.cassandra.gms", "org.apache.cassandra.hints", "org.apache.cassandra.schema", "org.apache.cassandra.service.StorageService", "org.apache.cassandra.db.SystemKeyspace", "org.apache.cassandra.batchlog.BatchlogManager", "org.apache.cassandra.net.MessagingService"});
      } else if(target.equals("repair")) {
         classQualifiers = Lists.newArrayList(new String[]{"org.apache.cassandra.repair", "org.apache.cassandra.db.compaction.CompactionManager", "org.apache.cassandra.service.SnapshotVerbHandler"});
      } else if(target.equals("streaming")) {
         classQualifiers = Lists.newArrayList(new String[]{"org.apache.cassandra.streaming", "org.apache.cassandra.dht.RangeStreamer"});
      } else if(target.equals("compaction")) {
         classQualifiers = Lists.newArrayList(new String[]{"org.apache.cassandra.db.compaction", "org.apache.cassandra.db.ColumnFamilyStore", "org.apache.cassandra.io.sstable.IndexSummaryRedistribution"});
      } else if(target.equals("cql")) {
         classQualifiers = Lists.newArrayList(new String[]{"org.apache.cassandra.cql3", "org.apache.cassandra.auth", "org.apache.cassandra.batchlog", "org.apache.cassandra.net.ResponseVerbHandler", "org.apache.cassandra.service.AbstractReadExecutor", "org.apache.cassandra.service.AbstractWriteResponseHandler", "org.apache.cassandra.service.paxos", "org.apache.cassandra.service.ReadCallback", "org.apache.cassandra.service.ResponseResolver"});
      } else if(target.equals("ring")) {
         classQualifiers = Lists.newArrayList(new String[]{"org.apache.cassandra.gms", "org.apache.cassandra.service.PendingRangeCalculatorService", "org.apache.cassandra.service.LoadBroadcaster", "org.apache.cassandra.transport.Server"});
      }

      Iterator var5 = ((List)classQualifiers).iterator();

      while(var5.hasNext()) {
         String classQualifier = (String)var5.next();
         probe.setLoggingLevel(classQualifier, level);
      }

   }
}
