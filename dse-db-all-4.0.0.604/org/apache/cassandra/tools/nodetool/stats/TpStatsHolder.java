package org.apache.cassandra.tools.nodetool.stats;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.function.ToLongFunction;
import java.util.regex.Pattern;
import org.apache.cassandra.tools.NodeProbe;

public class TpStatsHolder implements StatsHolder {
   public final NodeProbe probe;
   public final boolean includeTPCCores;
   static Pattern TPCCoreInfo = Pattern.compile("TPC/(other|\\d+).*");

   public TpStatsHolder(NodeProbe probe, boolean includeTPCCores) {
      this.probe = probe;
      this.includeTPCCores = includeTPCCores;
   }

   public Map<String, Object> convert2Map() {
      HashMap<String, Object> result = new HashMap();
      TreeMap<String, Map<String, Object>> threadPools = new TreeMap();
      HashMap<String, Object> droppedMessage = new HashMap();
      HashMap<String, double[]> waitLatencies = new HashMap();
      Iterator var5 = this.probe.getThreadPools().entries().iterator();

      while(true) {
         Entry tp;
         do {
            if(!var5.hasNext()) {
               result.put("ThreadPools", threadPools);
               var5 = this.probe.getDroppedMessages().entrySet().iterator();

               while(var5.hasNext()) {
                  tp = (Entry)var5.next();
                  droppedMessage.put(tp.getKey(), tp.getValue());

                  try {
                     waitLatencies.put(tp.getKey(), this.probe.metricPercentilesAsArray(this.probe.getMessagingQueueWaitMetrics((String)tp.getKey())));
                  } catch (RuntimeException var8) {
                     ;
                  }
               }

               result.put("DroppedMessage", droppedMessage);
               result.put("WaitLatencies", waitLatencies);
               return result;
            }

            tp = (Entry)var5.next();
         } while(!this.includeTPCCores && TPCCoreInfo.matcher((CharSequence)tp.getValue()).matches());

         HashMap<String, Object> threadPool = new HashMap();
         threadPool.put("ActiveTasks", this.probe.getThreadPoolMetric((String)tp.getKey(), (String)tp.getValue(), "ActiveTasks"));
         threadPool.put("PendingTasks", this.probe.getThreadPoolMetric((String)tp.getKey(), (String)tp.getValue(), "PendingTasks") + " (" + this.probe.getThreadPoolMetric((String)tp.getKey(), (String)tp.getValue(), "TotalBackpressureCountedTasks") + ")");
         threadPool.put("DelayedTasks", this.probe.getThreadPoolMetric((String)tp.getKey(), (String)tp.getValue(), "TotalBackpressureDelayedTasks"));
         threadPool.put("CompletedTasks", this.probe.getThreadPoolMetric((String)tp.getKey(), (String)tp.getValue(), "CompletedTasks"));
         threadPool.put("CurrentlyBlockedTasks", this.probe.getThreadPoolMetric((String)tp.getKey(), (String)tp.getValue(), "CurrentlyBlockedTasks"));
         threadPool.put("TotalBlockedTasks", this.probe.getThreadPoolMetric((String)tp.getKey(), (String)tp.getValue(), "TotalBlockedTasks"));
         if(threadPool.values().stream().mapToLong((x) -> {
            return x instanceof Number?((Number)x).longValue():0L;
         }).sum() > 0L) {
            threadPools.put(tp.getValue(), threadPool);
         }
      }
   }
}
