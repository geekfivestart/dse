package org.apache.cassandra.tools.nodetool.stats;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.cassandra.concurrent.TPCTaskType;

public class TpStatsPrinter {
   public TpStatsPrinter() {
   }

   public static StatsPrinter from(String format) {
      byte var2 = -1;
      switch(format.hashCode()) {
      case 3271912:
         if(format.equals("json")) {
            var2 = 0;
         }
         break;
      case 3701415:
         if(format.equals("yaml")) {
            var2 = 1;
         }
      }

      switch(var2) {
      case 0:
         return new StatsPrinter.JsonPrinter();
      case 1:
         return new StatsPrinter.YamlPrinter();
      default:
         return new TpStatsPrinter.DefaultPrinter();
      }
   }

   public static int longestTPCStatNameLength() {
      return longestStrLength((Collection)Arrays.stream(TPCTaskType.values()).map((t) -> {
         return t.loggedEventName;
      }).collect(Collectors.toList())) + 10;
   }

   public static int longestStrLength(Collection<Object> coll) {
      int maxLength = 0;

      Object o;
      for(Iterator var2 = coll.iterator(); var2.hasNext(); maxLength = Integer.max(maxLength, o.toString().length())) {
         o = var2.next();
      }

      return maxLength;
   }

   public static class DefaultPrinter implements StatsPrinter<TpStatsHolder> {
      public DefaultPrinter() {
      }

      public void print(TpStatsHolder data, PrintStream out) {
         Map<String, Object> convertData = data.convert2Map();
         String headerFormat = "%-" + TpStatsPrinter.longestTPCStatNameLength() + "s%12s%30s%10s%15s%10s%18s%n";
         out.printf(headerFormat, new Object[]{"Pool Name", "Active", "Pending (w/Backpressure)", "Delayed", "Completed", "Blocked", "All time blocked"});
         Map<Object, Object> threadPools = convertData.get("ThreadPools") instanceof Map?(Map)convertData.get("ThreadPools"):Collections.emptyMap();
         Iterator var6 = threadPools.entrySet().iterator();

         Map waitLatencies;
         while(var6.hasNext()) {
            Entry<Object, Object> entry = (Entry)var6.next();
            waitLatencies = entry.getValue() instanceof Map?(Map)entry.getValue():Collections.emptyMap();
            out.printf(headerFormat, new Object[]{entry.getKey(), waitLatencies.get("ActiveTasks"), waitLatencies.get("PendingTasks"), waitLatencies.get("DelayedTasks"), waitLatencies.get("CompletedTasks"), waitLatencies.get("CurrentlyBlockedTasks"), waitLatencies.get("TotalBlockedTasks")});
         }

         Map<Object, Object> droppedMessages = convertData.get("DroppedMessage") instanceof Map?(Map)convertData.get("DroppedMessage"):Collections.emptyMap();
         int messageTypeIndent = TpStatsPrinter.longestStrLength(droppedMessages.keySet()) + 5;
         out.printf("%n%-" + messageTypeIndent + "s%10s%18s%18s%18s%18s%n", new Object[]{"Message type", "Dropped", "", "Latency waiting in queue (micros)", "", ""});
         out.printf("%-" + messageTypeIndent + "s%10s%18s%18s%18s%18s%n", new Object[]{"", "", "50%", "95%", "99%", "Max"});
         waitLatencies = convertData.get("WaitLatencies") instanceof Map?(Map)convertData.get("WaitLatencies"):Collections.emptyMap();

         for(Iterator var9 = droppedMessages.entrySet().iterator(); var9.hasNext(); out.printf("%n", new Object[0])) {
            Entry<Object, Object> entry = (Entry)var9.next();
            out.printf("%-" + messageTypeIndent + "s%10s", new Object[]{entry.getKey(), entry.getValue()});
            if(waitLatencies.containsKey(entry.getKey())) {
               double[] latencies = (double[])waitLatencies.get(entry.getKey());
               out.printf("%18.2f%18.2f%18.2f%18.2f", new Object[]{Double.valueOf(latencies[0]), Double.valueOf(latencies[2]), Double.valueOf(latencies[4]), Double.valueOf(latencies[6])});
            } else {
               out.printf("%18s%18s%18s%18s", new Object[]{"N/A", "N/A", "N/A", "N/A"});
            }
         }

      }
   }
}
