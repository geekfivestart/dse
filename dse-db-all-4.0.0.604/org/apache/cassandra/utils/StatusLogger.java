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
      final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
      final String headerFormat = "%-" + TpStatsPrinter.longestTPCStatNameLength() + "s%12s%30s%10s%15s%10s%18s%n";
      StatusLogger.logger.info(String.format(headerFormat, "Pool Name", "Active", "Pending (w/Backpressure)", "Delayed", "Completed", "Blocked", "All Time Blocked"));
      final Multimap<String, String> jmxThreadPools = ThreadPoolMetrics.getJmxThreadPools(server);
      for (final String poolKey : jmxThreadPools.keySet()) {
         final TreeSet<String> poolValues = new TreeSet<String>(jmxThreadPools.get(poolKey));
         for (final String poolValue : poolValues) {
            StatusLogger.logger.info(String.format(headerFormat, poolValue, ThreadPoolMetrics.getJmxMetric(server, poolKey, poolValue, "ActiveTasks"), ThreadPoolMetrics.getJmxMetric(server, poolKey, poolValue, "PendingTasks") + " (" + ThreadPoolMetrics.getJmxMetric(server, poolKey, poolValue, "TotalBackpressureCountedTasks") + ")", ThreadPoolMetrics.getJmxMetric(server, poolKey, poolValue, "TotalBackpressureDelayedTasks"), ThreadPoolMetrics.getJmxMetric(server, poolKey, poolValue, "CompletedTasks"), ThreadPoolMetrics.getJmxMetric(server, poolKey, poolValue, "CurrentlyBlockedTasks"), ThreadPoolMetrics.getJmxMetric(server, poolKey, poolValue, "TotalBlockedTasks")));
         }
      }
      StatusLogger.logger.info(String.format("%-25s%10s%10s", "CompactionManager", CompactionManager.instance.getActiveCompactions(), CompactionManager.instance.getPendingTasks()));
      int pendingLargeMessages = 0;
      for (final int n : MessagingService.instance().getLargeMessagePendingTasks().values()) {
         pendingLargeMessages += n;
      }
      int pendingSmallMessages = 0;
      for (final int n2 : MessagingService.instance().getSmallMessagePendingTasks().values()) {
         pendingSmallMessages += n2;
      }
      StatusLogger.logger.info(String.format("%-25s%10s%10s", "MessagingService", "n/a", pendingLargeMessages + "/" + pendingSmallMessages));
      StatusLogger.logger.info("Global file buffer pool size: {}", (Object)FBUtilities.prettyPrintMemory(BufferPool.sizeInBytes()));
      StatusLogger.logger.info("Global memtable buffer pool size: onHeap = {}, offHeap = {}", (Object)FBUtilities.prettyPrintMemory(Memtable.MEMORY_POOL.onHeap.used()), (Object)FBUtilities.prettyPrintMemory(Memtable.MEMORY_POOL.offHeap.used()));
      final AutoSavingCache<KeyCacheKey, BigRowIndexEntry> keyCache = CacheService.instance.keyCache;
      final AutoSavingCache<RowCacheKey, IRowCacheEntry> rowCache = CacheService.instance.rowCache;
      final int keyCacheKeysToSave = DatabaseDescriptor.getKeyCacheKeysToSave();
      final int rowCacheKeysToSave = DatabaseDescriptor.getRowCacheKeysToSave();
      StatusLogger.logger.info(String.format("%-25s%10s%25s%25s", "Cache Type", "Size", "Capacity", "KeysToSave"));
      StatusLogger.logger.info(String.format("%-25s%10s%25s%25s", "KeyCache", keyCache.weightedSize(), keyCache.getCapacity(), (keyCacheKeysToSave == Integer.MAX_VALUE) ? "all" : keyCacheKeysToSave));
      StatusLogger.logger.info(String.format("%-25s%10s%25s%25s", "RowCache", rowCache.weightedSize(), rowCache.getCapacity(), (rowCacheKeysToSave == Integer.MAX_VALUE) ? "all" : rowCacheKeysToSave));
      StatusLogger.logger.info(String.format("%-25s%20s", "Table", "Memtable ops,data"));
      for (final ColumnFamilyStore cfs : ColumnFamilyStore.all()) {
         StatusLogger.logger.info(String.format("%-25s%20s", cfs.keyspace.getName() + "." + cfs.name, cfs.metric.memtableColumnsCount.getValue() + "," + cfs.metric.memtableLiveDataSize.getValue()));
      }
   }
}
