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
      HashMap<String, Object> result = new HashMap<String, Object>();
      TreeMap threadPools = new TreeMap();
      HashMap droppedMessage = new HashMap();
      HashMap waitLatencies = new HashMap();
      for (Map.Entry tp : this.probe.getThreadPools().entries()) {
         if (!this.includeTPCCores && TPCCoreInfo.matcher((CharSequence)tp.getValue()).matches()) continue;
         HashMap<String, Object> threadPool = new HashMap<String, Object>();
         threadPool.put("ActiveTasks", this.probe.getThreadPoolMetric((String)tp.getKey(), (String)tp.getValue(), "ActiveTasks"));
         threadPool.put("PendingTasks", this.probe.getThreadPoolMetric((String)tp.getKey(), (String)tp.getValue(), "PendingTasks") + " (" + this.probe.getThreadPoolMetric((String)tp.getKey(), (String)tp.getValue(), "TotalBackpressureCountedTasks") + ")");
         threadPool.put("DelayedTasks", this.probe.getThreadPoolMetric((String)tp.getKey(), (String)tp.getValue(), "TotalBackpressureDelayedTasks"));
         threadPool.put("CompletedTasks", this.probe.getThreadPoolMetric((String)tp.getKey(), (String)tp.getValue(), "CompletedTasks"));
         threadPool.put("CurrentlyBlockedTasks", this.probe.getThreadPoolMetric((String)tp.getKey(), (String)tp.getValue(), "CurrentlyBlockedTasks"));
         threadPool.put("TotalBlockedTasks", this.probe.getThreadPoolMetric((String)tp.getKey(), (String)tp.getValue(), "TotalBlockedTasks"));
         if (threadPool.values().stream().mapToLong(x -> x instanceof Number ? ((Number)x).longValue() : 0L).sum() <= 0L) continue;
         threadPools.put(tp.getValue(), threadPool);
      }
      result.put("ThreadPools", threadPools);
      for (Map.Entry entry : this.probe.getDroppedMessages().entrySet()) {
         droppedMessage.put(entry.getKey(), entry.getValue());
         try {
            waitLatencies.put(entry.getKey(), this.probe.metricPercentilesAsArray(this.probe.getMessagingQueueWaitMetrics((String)entry.getKey())));
         }
         catch (RuntimeException threadPool) {}
      }
      result.put("DroppedMessage", droppedMessage);
      result.put("WaitLatencies", waitLatencies);
      return result;
   }
}
