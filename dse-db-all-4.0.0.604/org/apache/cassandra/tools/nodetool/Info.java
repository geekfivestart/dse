package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import java.lang.management.MemoryUsage;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import javax.management.InstanceNotFoundException;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.CacheServiceMBean;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "info",
   description = "Print node information (uptime, load, ...)"
)
public class Info extends NodeTool.NodeToolCmd {
   @Option(
      name = {"-T", "--tokens"},
      description = "Display all tokens"
   )
   private boolean tokens = false;

   public Info() {
   }

   public void execute(NodeProbe probe) {
      boolean gossipInitialized = probe.isGossipRunning();
      System.out.printf("%-23s: %s%n", new Object[]{"ID", probe.getLocalHostId()});
      System.out.printf("%-23s: %s%n", new Object[]{"Gossip active", Boolean.valueOf(gossipInitialized)});
      System.out.printf("%-23s: %s%n", new Object[]{"Native Transport active", Boolean.valueOf(probe.isNativeTransportRunning())});
      System.out.printf("%-23s: %s%n", new Object[]{"Load", probe.getLoadString()});
      if(gossipInitialized) {
         System.out.printf("%-23s: %s%n", new Object[]{"Generation No", Integer.valueOf(probe.getCurrentGenerationNumber())});
      } else {
         System.out.printf("%-23s: %s%n", new Object[]{"Generation No", Integer.valueOf(0)});
      }

      long secondsUp = probe.getUptime() / 1000L;
      System.out.printf("%-23s: %d%n", new Object[]{"Uptime (seconds)", Long.valueOf(secondsUp)});
      MemoryUsage heapUsage = probe.getHeapMemoryUsage();
      double memUsed = (double)heapUsage.getUsed() / 1048576.0D;
      double memMax = (double)heapUsage.getMax() / 1048576.0D;
      System.out.printf("%-23s: %.2f / %.2f%n", new Object[]{"Heap Memory (MB)", Double.valueOf(memUsed), Double.valueOf(memMax)});

      try {
         System.out.printf("%-23s: %.2f%n", new Object[]{"Off Heap Memory (MB)", Double.valueOf(getOffHeapMemoryUsed(probe))});
      } catch (RuntimeException var16) {
         if(!(var16.getCause() instanceof InstanceNotFoundException)) {
            throw var16;
         }
      }

      System.out.printf("%-23s: %s%n", new Object[]{"Data Center", probe.getDataCenter()});
      System.out.printf("%-23s: %s%n", new Object[]{"Rack", probe.getRack()});
      System.out.printf("%-23s: %s%n", new Object[]{"Exceptions", Long.valueOf(probe.getStorageMetric("Exceptions"))});
      CacheServiceMBean cacheService = probe.getCacheServiceMBean();

      try {
         System.out.printf("%-23s: entries %d, size %s, capacity %s, %d hits, %d requests, %.3f recent hit rate, %d save period in seconds%n", new Object[]{"Key Cache", probe.getCacheMetric("KeyCache", "Entries"), FileUtils.stringifyFileSize((double)((Long)probe.getCacheMetric("KeyCache", "Size")).longValue()), FileUtils.stringifyFileSize((double)((Long)probe.getCacheMetric("KeyCache", "Capacity")).longValue()), probe.getCacheMetric("KeyCache", "Hits"), probe.getCacheMetric("KeyCache", "Requests"), probe.getCacheMetric("KeyCache", "HitRate"), Integer.valueOf(cacheService.getKeyCacheSavePeriodInSeconds())});
      } catch (RuntimeException var15) {
         if(!(var15.getCause() instanceof InstanceNotFoundException)) {
            throw var15;
         }
      }

      System.out.printf("%-23s: entries %d, size %s, capacity %s, %d hits, %d requests, %.3f recent hit rate, %d save period in seconds%n", new Object[]{"Row Cache", probe.getCacheMetric("RowCache", "Entries"), FileUtils.stringifyFileSize((double)((Long)probe.getCacheMetric("RowCache", "Size")).longValue()), FileUtils.stringifyFileSize((double)((Long)probe.getCacheMetric("RowCache", "Capacity")).longValue()), probe.getCacheMetric("RowCache", "Hits"), probe.getCacheMetric("RowCache", "Requests"), probe.getCacheMetric("RowCache", "HitRate"), Integer.valueOf(cacheService.getRowCacheSavePeriodInSeconds())});
      System.out.printf("%-23s: entries %d, size %s, capacity %s, %d hits, %d requests, %.3f recent hit rate, %d save period in seconds%n", new Object[]{"Counter Cache", probe.getCacheMetric("CounterCache", "Entries"), FileUtils.stringifyFileSize((double)((Long)probe.getCacheMetric("CounterCache", "Size")).longValue()), FileUtils.stringifyFileSize((double)((Long)probe.getCacheMetric("CounterCache", "Capacity")).longValue()), probe.getCacheMetric("CounterCache", "Hits"), probe.getCacheMetric("CounterCache", "Requests"), probe.getCacheMetric("CounterCache", "HitRate"), Integer.valueOf(cacheService.getCounterCacheSavePeriodInSeconds())});

      try {
         System.out.printf("%-23s: entries %d, size %s, capacity %s, %d misses, %d requests, %.3f recent hit rate, %.3f %s miss latency%n", new Object[]{"Chunk Cache", probe.getCacheMetric("ChunkCache", "Entries"), FileUtils.stringifyFileSize((double)((Long)probe.getCacheMetric("ChunkCache", "Size")).longValue()), FileUtils.stringifyFileSize((double)((Long)probe.getCacheMetric("ChunkCache", "Capacity")).longValue()), probe.getCacheMetric("ChunkCache", "Misses"), probe.getCacheMetric("ChunkCache", "Requests"), probe.getCacheMetric("ChunkCache", "HitRate"), probe.getCacheMetric("ChunkCache", "MissLatency"), probe.getCacheMetric("ChunkCache", "MissLatencyUnit")});
      } catch (RuntimeException var14) {
         if(!(var14.getCause() instanceof InstanceNotFoundException)) {
            throw var14;
         }
      }

      System.out.printf("%-23s: %s%%%n", new Object[]{"Percent Repaired", probe.getColumnFamilyMetric((String)null, (String)null, "PercentRepaired")});
      if(probe.isJoined()) {
         List<String> tokens = probe.getTokens();
         if(tokens.size() != 1 && !this.tokens) {
            System.out.printf("%-23s: (invoke with -T/--tokens to see all %d tokens)%n", new Object[]{"Token", Integer.valueOf(tokens.size())});
         } else {
            Iterator var12 = tokens.iterator();

            while(var12.hasNext()) {
               String token = (String)var12.next();
               System.out.printf("%-23s: %s%n", new Object[]{"Token", token});
            }
         }
      } else {
         System.out.printf("%-23s: (node is not joined to the cluster)%n", new Object[]{"Token"});
      }

   }

   private static double getOffHeapMemoryUsed(NodeProbe probe) {
      long offHeapMemUsedInBytes = 0L;

      String keyspaceName;
      String cfName;
      for(Iterator cfamilies = probe.getColumnFamilyStoreMBeanProxies(); cfamilies.hasNext(); offHeapMemUsedInBytes += ((Long)probe.getColumnFamilyMetric(keyspaceName, cfName, "CompressionMetadataOffHeapMemoryUsed")).longValue()) {
         Entry<String, ColumnFamilyStoreMBean> entry = (Entry)cfamilies.next();
         keyspaceName = (String)entry.getKey();
         cfName = ((ColumnFamilyStoreMBean)entry.getValue()).getTableName();
         offHeapMemUsedInBytes += ((Long)probe.getColumnFamilyMetric(keyspaceName, cfName, "MemtableOffHeapSize")).longValue();
         offHeapMemUsedInBytes += ((Long)probe.getColumnFamilyMetric(keyspaceName, cfName, "BloomFilterOffHeapMemoryUsed")).longValue();
         offHeapMemUsedInBytes += ((Long)probe.getColumnFamilyMetric(keyspaceName, cfName, "IndexSummaryOffHeapMemoryUsed")).longValue();
      }

      return (double)offHeapMemUsedInBytes / 1048576.0D;
   }
}
