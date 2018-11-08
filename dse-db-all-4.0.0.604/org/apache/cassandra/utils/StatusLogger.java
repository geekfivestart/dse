package org.apache.cassandra.utils;

import com.google.common.collect.Multimap;
import java.lang.management.ManagementFactory;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantLock;
import javax.management.MBeanServer;
import org.apache.cassandra.cache.AutoSavingCache;
import org.apache.cassandra.cache.IRowCacheEntry;
import org.apache.cassandra.cache.KeyCacheKey;
import org.apache.cassandra.cache.RowCacheKey;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.io.sstable.format.big.BigRowIndexEntry;
import org.apache.cassandra.metrics.ThreadPoolMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.tools.nodetool.stats.TpStatsPrinter;
import org.apache.cassandra.utils.memory.BufferPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatusLogger {
   private static final Logger logger = LoggerFactory.getLogger(StatusLogger.class);
   private static final ReentrantLock busyMonitor = new ReentrantLock();

   public StatusLogger() {
   }

   public static void log() {
      if(busyMonitor.tryLock()) {
         try {
            logStatus();
         } finally {
            busyMonitor.unlock();
         }
      } else {
         logger.trace("StatusLogger is busy");
      }

   }

   private static void logStatus() {
      MBeanServer server = ManagementFactory.getPlatformMBeanServer();
      String headerFormat = "%-" + TpStatsPrinter.longestTPCStatNameLength() + "s%12s%30s%10s%15s%10s%18s%n";
      logger.info(String.format(headerFormat, new Object[]{"Pool Name", "Active", "Pending (w/Backpressure)", "Delayed", "Completed", "Blocked", "All Time Blocked"}));
      Multimap<String, String> jmxThreadPools = ThreadPoolMetrics.getJmxThreadPools(server);
      Iterator var3 = jmxThreadPools.keySet().iterator();

      while(var3.hasNext()) {
         String poolKey = (String)var3.next();
         TreeSet<String> poolValues = new TreeSet(jmxThreadPools.get(poolKey));
         Iterator var6 = poolValues.iterator();

         while(var6.hasNext()) {
            String poolValue = (String)var6.next();
            logger.info(String.format(headerFormat, new Object[]{poolValue, ThreadPoolMetrics.getJmxMetric(server, poolKey, poolValue, "ActiveTasks"), ThreadPoolMetrics.getJmxMetric(server, poolKey, poolValue, "PendingTasks") + " (" + ThreadPoolMetrics.getJmxMetric(server, poolKey, poolValue, "TotalBackpressureCountedTasks") + ")", ThreadPoolMetrics.getJmxMetric(server, poolKey, poolValue, "TotalBackpressureDelayedTasks"), ThreadPoolMetrics.getJmxMetric(server, poolKey, poolValue, "CompletedTasks"), ThreadPoolMetrics.getJmxMetric(server, poolKey, poolValue, "CurrentlyBlockedTasks"), ThreadPoolMetrics.getJmxMetric(server, poolKey, poolValue, "TotalBlockedTasks")}));
         }
      }

      logger.info(String.format("%-25s%10s%10s", new Object[]{"CompactionManager", Integer.valueOf(CompactionManager.instance.getActiveCompactions()), Integer.valueOf(CompactionManager.instance.getPendingTasks())}));
      int pendingLargeMessages = 0;

      int n;
      for(Iterator var12 = MessagingService.instance().getLargeMessagePendingTasks().values().iterator(); var12.hasNext(); pendingLargeMessages += n) {
         n = ((Integer)var12.next()).intValue();
      }

      int pendingSmallMessages = 0;

      int n;
      for(Iterator var15 = MessagingService.instance().getSmallMessagePendingTasks().values().iterator(); var15.hasNext(); pendingSmallMessages += n) {
         n = ((Integer)var15.next()).intValue();
      }

      logger.info(String.format("%-25s%10s%10s", new Object[]{"MessagingService", "n/a", pendingLargeMessages + "/" + pendingSmallMessages}));
      logger.info("Global file buffer pool size: {}", FBUtilities.prettyPrintMemory(BufferPool.sizeInBytes()));
      logger.info("Global memtable buffer pool size: onHeap = {}, offHeap = {}", FBUtilities.prettyPrintMemory(Memtable.MEMORY_POOL.onHeap.used()), FBUtilities.prettyPrintMemory(Memtable.MEMORY_POOL.offHeap.used()));
      AutoSavingCache<KeyCacheKey, BigRowIndexEntry> keyCache = CacheService.instance.keyCache;
      AutoSavingCache<RowCacheKey, IRowCacheEntry> rowCache = CacheService.instance.rowCache;
      int keyCacheKeysToSave = DatabaseDescriptor.getKeyCacheKeysToSave();
      int rowCacheKeysToSave = DatabaseDescriptor.getRowCacheKeysToSave();
      logger.info(String.format("%-25s%10s%25s%25s", new Object[]{"Cache Type", "Size", "Capacity", "KeysToSave"}));
      logger.info(String.format("%-25s%10s%25s%25s", new Object[]{"KeyCache", Long.valueOf(keyCache.weightedSize()), Long.valueOf(keyCache.getCapacity()), keyCacheKeysToSave == 2147483647?"all":Integer.valueOf(keyCacheKeysToSave)}));
      logger.info(String.format("%-25s%10s%25s%25s", new Object[]{"RowCache", Long.valueOf(rowCache.weightedSize()), Long.valueOf(rowCache.getCapacity()), rowCacheKeysToSave == 2147483647?"all":Integer.valueOf(rowCacheKeysToSave)}));
      logger.info(String.format("%-25s%20s", new Object[]{"Table", "Memtable ops,data"}));
      Iterator var9 = ColumnFamilyStore.all().iterator();

      while(var9.hasNext()) {
         ColumnFamilyStore cfs = (ColumnFamilyStore)var9.next();
         logger.info(String.format("%-25s%20s", new Object[]{cfs.keyspace.getName() + "." + cfs.name, cfs.metric.memtableColumnsCount.getValue() + "," + cfs.metric.memtableLiveDataSize.getValue()}));
      }

   }
}
