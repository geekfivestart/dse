package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

@Command(
   name = "compactionstats",
   description = "Print statistics on compactions"
)
public class CompactionStats extends NodeTool.NodeToolCmd {
   @Option(
      title = "human_readable",
      name = {"-H", "--human-readable"},
      description = "Display bytes in human readable form, i.e. KiB, MiB, GiB, TiB"
   )
   private boolean humanReadable = false;

   public CompactionStats() {
   }

   public void execute(NodeProbe probe) {
      CompactionManagerMBean cm = probe.getCompactionManagerProxy();
      Map<String, Map<String, Integer>> pendingTaskNumberByTable = (Map)probe.getCompactionMetric("PendingTasksByTableName");
      int numTotalPendingTask = 0;
      Iterator var5 = pendingTaskNumberByTable.entrySet().iterator();

      Entry ksEntry;
      while(var5.hasNext()) {
         ksEntry = (Entry)var5.next();

         Entry tableEntry;
         for(Iterator var7 = ((Map)ksEntry.getValue()).entrySet().iterator(); var7.hasNext(); numTotalPendingTask += ((Integer)tableEntry.getValue()).intValue()) {
            tableEntry = (Entry)var7.next();
         }
      }

      System.out.println("pending tasks: " + numTotalPendingTask);
      var5 = pendingTaskNumberByTable.entrySet().iterator();

      while(var5.hasNext()) {
         ksEntry = (Entry)var5.next();
         String ksName = (String)ksEntry.getKey();
         Iterator var13 = ((Map)ksEntry.getValue()).entrySet().iterator();

         while(var13.hasNext()) {
            Entry<String, Integer> tableEntry = (Entry)var13.next();
            String tableName = (String)tableEntry.getKey();
            int pendingTaskCount = ((Integer)tableEntry.getValue()).intValue();
            System.out.println("- " + ksName + '.' + tableName + ": " + pendingTaskCount);
         }
      }

      System.out.println();
      reportCompactionTable(cm.getCompactions(), probe.getCompactionThroughput(), this.humanReadable);
   }

   public static void reportCompactionTable(List<Map<String, String>> compactions, int compactionThroughput, boolean humanReadable) {
      if(!compactions.isEmpty()) {
         long remainingBytes = 0L;
         TableBuilder table = new TableBuilder();
         table.add(new String[]{"id", "compaction type", "keyspace", "table", "completed", "total", "unit", "progress"});
         Iterator var6 = compactions.iterator();

         while(var6.hasNext()) {
            Map<String, String> c = (Map)var6.next();
            long total = Long.parseLong((String)c.get("total"));
            long completed = Long.parseLong((String)c.get("completed"));
            String taskType = (String)c.get("taskType");
            String keyspace = (String)c.get("keyspace");
            String columnFamily = (String)c.get("columnfamily");
            String unit = (String)c.get("unit");
            boolean toFileSize = humanReadable && CompactionInfo.Unit.isFileSize(unit);
            String completedStr = toFileSize?FileUtils.stringifyFileSize((double)completed):Long.toString(completed);
            String totalStr = toFileSize?FileUtils.stringifyFileSize((double)total):Long.toString(total);
            String percentComplete = total == 0L?"n/a":(new DecimalFormat("0.00")).format((double)completed / (double)total * 100.0D) + "%";
            String id = (String)c.get("compactionId");
            table.add(new String[]{id, taskType, keyspace, columnFamily, completedStr, totalStr, unit, percentComplete});
            if(taskType.equals(OperationType.COMPACTION.toString())) {
               remainingBytes += total - completed;
            }
         }

         table.printTo(System.out);
         String remainingTime = "n/a";
         if(compactionThroughput != 0) {
            long remainingTimeInSecs = remainingBytes / (1048576L * (long)compactionThroughput);
            remainingTime = String.format("%dh%02dm%02ds", new Object[]{Long.valueOf(remainingTimeInSecs / 3600L), Long.valueOf(remainingTimeInSecs % 3600L / 60L), Long.valueOf(remainingTimeInSecs % 60L)});
         }

         System.out.printf("%25s%10s%n", new Object[]{"Active compaction remaining time : ", remainingTime});
      }

   }
}
