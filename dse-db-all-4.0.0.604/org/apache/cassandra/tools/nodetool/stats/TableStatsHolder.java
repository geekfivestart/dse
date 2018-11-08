package org.apache.cassandra.tools.nodetool.stats;

import com.google.common.collect.ArrayListMultimap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.management.InstanceNotFoundException;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.tools.NodeProbe;

public class TableStatsHolder implements StatsHolder {
   public final List<StatsKeyspace> keyspaces = new ArrayList();
   public final int numberOfTables;

   public TableStatsHolder(NodeProbe probe, boolean humanReadable, boolean ignore, List<String> tableNames) {
      this.numberOfTables = probe.getNumberOfTables();
      this.initializeKeyspaces(probe, humanReadable, ignore, tableNames);
   }

   public Map<String, Object> convert2Map() {
      HashMap<String, Object> mpRet = new HashMap();
      mpRet.put("total_number_of_tables", Integer.valueOf(this.numberOfTables));
      Iterator var2 = this.keyspaces.iterator();

      while(var2.hasNext()) {
         StatsKeyspace keyspace = (StatsKeyspace)var2.next();
         HashMap<String, Object> mpKeyspace = new HashMap();
         mpKeyspace.put("read_latency", Double.valueOf(keyspace.readLatency()));
         mpKeyspace.put("read_count", Long.valueOf(keyspace.readCount));
         mpKeyspace.put("read_latency_ms", Double.valueOf(keyspace.readLatency()));
         mpKeyspace.put("write_count", Long.valueOf(keyspace.writeCount));
         mpKeyspace.put("write_latency_ms", Double.valueOf(keyspace.writeLatency()));
         mpKeyspace.put("pending_flushes", Integer.valueOf(keyspace.pendingFlushes));
         List<StatsTable> tables = keyspace.tables;
         Map<String, Map<String, Object>> mpTables = new HashMap();
         Iterator var7 = tables.iterator();

         while(var7.hasNext()) {
            StatsTable table = (StatsTable)var7.next();
            Map<String, Object> mpTable = new HashMap();
            mpTable.put("sstables_in_each_level", table.sstablesInEachLevel);
            mpTable.put("space_used_live", table.spaceUsedLive);
            mpTable.put("space_used_total", table.spaceUsedTotal);
            mpTable.put("space_used_by_snapshots_total", table.spaceUsedBySnapshotsTotal);
            if(table.offHeapUsed) {
               mpTable.put("off_heap_memory_used_total", table.offHeapMemoryUsedTotal);
            }

            mpTable.put("sstable_compression_ratio", table.sstableCompressionRatio);
            mpTable.put("number_of_partitions_estimate", table.numberOfPartitionsEstimate);
            mpTable.put("memtable_cell_count", table.memtableCellCount);
            mpTable.put("memtable_data_size", table.memtableDataSize);
            if(table.memtableOffHeapUsed) {
               mpTable.put("memtable_off_heap_memory_used", table.memtableOffHeapMemoryUsed);
            }

            mpTable.put("memtable_switch_count", table.memtableSwitchCount);
            mpTable.put("local_read_count", Long.valueOf(table.localReadCount));
            mpTable.put("local_read_latency_ms", String.format("%01.3f", new Object[]{Double.valueOf(table.localReadLatencyMs)}));
            mpTable.put("local_write_count", Long.valueOf(table.localWriteCount));
            mpTable.put("local_write_latency_ms", String.format("%01.3f", new Object[]{Double.valueOf(table.localWriteLatencyMs)}));
            mpTable.put("pending_flushes", table.pendingFlushes);
            mpTable.put("percent_repaired", Double.valueOf(table.percentRepaired));
            mpTable.put("bytes_repaired", Long.valueOf(table.bytesRepaired));
            mpTable.put("bytes_unrepaired", Long.valueOf(table.bytesUnrepaired));
            mpTable.put("bytes_pending_repair", Long.valueOf(table.bytesPendingRepair));
            mpTable.put("bloom_filter_false_positives", table.bloomFilterFalsePositives);
            mpTable.put("bloom_filter_false_ratio", String.format("%01.5f", new Object[]{table.bloomFilterFalseRatio}));
            mpTable.put("bloom_filter_space_used", table.bloomFilterSpaceUsed);
            if(table.bloomFilterOffHeapUsed) {
               mpTable.put("bloom_filter_off_heap_memory_used", table.bloomFilterOffHeapMemoryUsed);
            }

            if(table.indexSummaryOffHeapUsed) {
               mpTable.put("index_summary_off_heap_memory_used", table.indexSummaryOffHeapMemoryUsed);
            }

            if(table.compressionMetadataOffHeapUsed) {
               mpTable.put("compression_metadata_off_heap_memory_used", table.compressionMetadataOffHeapMemoryUsed);
            }

            mpTable.put("compacted_partition_minimum_bytes", Long.valueOf(table.compactedPartitionMinimumBytes));
            mpTable.put("compacted_partition_maximum_bytes", Long.valueOf(table.compactedPartitionMaximumBytes));
            mpTable.put("compacted_partition_mean_bytes", Long.valueOf(table.compactedPartitionMeanBytes));
            mpTable.put("average_live_cells_per_slice_last_five_minutes", Double.valueOf(table.averageLiveCellsPerSliceLastFiveMinutes));
            mpTable.put("maximum_live_cells_per_slice_last_five_minutes", Long.valueOf(table.maximumLiveCellsPerSliceLastFiveMinutes));
            mpTable.put("average_tombstones_per_slice_last_five_minutes", Double.valueOf(table.averageTombstonesPerSliceLastFiveMinutes));
            mpTable.put("maximum_tombstones_per_slice_last_five_minutes", Long.valueOf(table.maximumTombstonesPerSliceLastFiveMinutes));
            mpTable.put("dropped_mutations", table.droppedMutations);
            mpTables.put(table.name, mpTable);
         }

         mpKeyspace.put("tables", mpTables);
         mpRet.put(keyspace.name, mpKeyspace);
      }

      return mpRet;
   }

   private void initializeKeyspaces(NodeProbe probe, boolean humanReadable, boolean ignore, List<String> tableNames) {
      TableStatsHolder.OptionFilter filter = new TableStatsHolder.OptionFilter(ignore, tableNames);
      ArrayListMultimap<String, ColumnFamilyStoreMBean> selectedTableMbeans = ArrayListMultimap.create();
      Map<String, StatsKeyspace> keyspaceStats = new HashMap();
      Iterator tableMBeans = probe.getColumnFamilyStoreMBeanProxies();

      while(tableMBeans.hasNext()) {
         Entry<String, ColumnFamilyStoreMBean> entry = (Entry)tableMBeans.next();
         String keyspaceName = (String)entry.getKey();
         ColumnFamilyStoreMBean tableProxy = (ColumnFamilyStoreMBean)entry.getValue();
         if(filter.isKeyspaceIncluded(keyspaceName)) {
            StatsKeyspace stats = (StatsKeyspace)keyspaceStats.get(keyspaceName);
            if(stats == null) {
               stats = new StatsKeyspace(probe, keyspaceName);
               keyspaceStats.put(keyspaceName, stats);
            }

            stats.add(tableProxy);
            if(filter.isTableIncluded(keyspaceName, tableProxy.getTableName())) {
               selectedTableMbeans.put(keyspaceName, tableProxy);
            }
         }
      }

      filter.verifyKeyspaces(probe.getKeyspaces());
      filter.verifyTables();
      Iterator var39 = selectedTableMbeans.asMap().entrySet().iterator();

      while(var39.hasNext()) {
         Entry<String, Collection<ColumnFamilyStoreMBean>> entry = (Entry)var39.next();
         String keyspaceName = (String)entry.getKey();
         Collection<ColumnFamilyStoreMBean> tables = (Collection)entry.getValue();
         StatsKeyspace statsKeyspace = (StatsKeyspace)keyspaceStats.get(keyspaceName);
         Iterator var14 = tables.iterator();

         while(var14.hasNext()) {
            ColumnFamilyStoreMBean table = (ColumnFamilyStoreMBean)var14.next();
            String tableName = table.getTableName();
            StatsTable statsTable = new StatsTable();
            statsTable.name = tableName;
            statsTable.isIndex = tableName.contains(".");
            statsTable.sstableCount = probe.getColumnFamilyMetric(keyspaceName, tableName, "LiveSSTableCount");
            int[] leveledSStables = table.getSSTableCountPerLevel();
            if(leveledSStables != null) {
               statsTable.isLeveledSstable = true;
               fillSStablesPerLevel(leveledSStables, table.getLevelFanoutSize(), statsTable.sstablesInEachLevel);
            }

            Long memtableOffHeapSize = null;
            Long bloomFilterOffHeapSize = null;
            Long indexSummaryOffHeapSize = null;
            Long compressionMetadataOffHeapSize = null;
            Long offHeapSize = null;
            Double percentRepaired = null;
            Long bytesRepaired = null;
            Long bytesUnrepaired = null;
            Long bytesPendingRepair = null;

            try {
               memtableOffHeapSize = (Long)probe.getColumnFamilyMetric(keyspaceName, tableName, "MemtableOffHeapSize");
               bloomFilterOffHeapSize = (Long)probe.getColumnFamilyMetric(keyspaceName, tableName, "BloomFilterOffHeapMemoryUsed");
               indexSummaryOffHeapSize = (Long)probe.getColumnFamilyMetric(keyspaceName, tableName, "IndexSummaryOffHeapMemoryUsed");
               compressionMetadataOffHeapSize = (Long)probe.getColumnFamilyMetric(keyspaceName, tableName, "CompressionMetadataOffHeapMemoryUsed");
               offHeapSize = Long.valueOf(memtableOffHeapSize.longValue() + bloomFilterOffHeapSize.longValue() + indexSummaryOffHeapSize.longValue() + compressionMetadataOffHeapSize.longValue());
               percentRepaired = (Double)probe.getColumnFamilyMetric(keyspaceName, tableName, "PercentRepaired");
               bytesRepaired = (Long)probe.getColumnFamilyMetric(keyspaceName, tableName, "BytesRepaired");
               bytesUnrepaired = (Long)probe.getColumnFamilyMetric(keyspaceName, tableName, "BytesUnrepaired");
               bytesPendingRepair = (Long)probe.getColumnFamilyMetric(keyspaceName, tableName, "BytesPendingRepair");
            } catch (RuntimeException var38) {
               if(!(var38.getCause() instanceof InstanceNotFoundException)) {
                  throw var38;
               }
            }

            statsTable.spaceUsedLive = this.format(((Long)probe.getColumnFamilyMetric(keyspaceName, tableName, "LiveDiskSpaceUsed")).longValue(), humanReadable);
            statsTable.spaceUsedTotal = this.format(((Long)probe.getColumnFamilyMetric(keyspaceName, tableName, "TotalDiskSpaceUsed")).longValue(), humanReadable);
            statsTable.spaceUsedBySnapshotsTotal = this.format(((Long)probe.getColumnFamilyMetric(keyspaceName, tableName, "SnapshotsSize")).longValue(), humanReadable);
            if(offHeapSize != null) {
               statsTable.offHeapUsed = true;
               statsTable.offHeapMemoryUsedTotal = this.format(offHeapSize.longValue(), humanReadable);
            }

            if(percentRepaired != null) {
               statsTable.percentRepaired = (double)Math.round(100.0D * percentRepaired.doubleValue()) / 100.0D;
            }

            statsTable.bytesRepaired = bytesRepaired != null?bytesRepaired.longValue():0L;
            statsTable.bytesUnrepaired = bytesUnrepaired != null?bytesUnrepaired.longValue():0L;
            statsTable.bytesPendingRepair = bytesPendingRepair != null?bytesPendingRepair.longValue():0L;
            statsTable.sstableCompressionRatio = probe.getColumnFamilyMetric(keyspaceName, tableName, "CompressionRatio");
            Object estimatedPartitionCount = probe.getColumnFamilyMetric(keyspaceName, tableName, "EstimatedPartitionCount");
            if(Long.valueOf(-1L).equals(estimatedPartitionCount)) {
               estimatedPartitionCount = Long.valueOf(0L);
            }

            statsTable.numberOfPartitionsEstimate = estimatedPartitionCount;
            statsTable.memtableCellCount = probe.getColumnFamilyMetric(keyspaceName, tableName, "MemtableColumnsCount");
            statsTable.memtableDataSize = this.format(((Long)probe.getColumnFamilyMetric(keyspaceName, tableName, "MemtableLiveDataSize")).longValue(), humanReadable);
            if(memtableOffHeapSize != null) {
               statsTable.memtableOffHeapUsed = true;
               statsTable.memtableOffHeapMemoryUsed = this.format(memtableOffHeapSize.longValue(), humanReadable);
            }

            statsTable.memtableSwitchCount = probe.getColumnFamilyMetric(keyspaceName, tableName, "MemtableSwitchCount");
            statsTable.localReadCount = ((CassandraMetricsRegistry.JmxTimerMBean)probe.getColumnFamilyMetric(keyspaceName, tableName, "ReadLatency")).getCount();
            double localReadLatency = ((CassandraMetricsRegistry.JmxTimerMBean)probe.getColumnFamilyMetric(keyspaceName, tableName, "ReadLatency")).getMean() / 1000.0D;
            double localRLatency = localReadLatency > 0.0D?localReadLatency:0.0D / 0.0;
            statsTable.localReadLatencyMs = localRLatency;
            statsTable.localWriteCount = ((CassandraMetricsRegistry.JmxTimerMBean)probe.getColumnFamilyMetric(keyspaceName, tableName, "WriteLatency")).getCount();
            double localWriteLatency = ((CassandraMetricsRegistry.JmxTimerMBean)probe.getColumnFamilyMetric(keyspaceName, tableName, "WriteLatency")).getMean() / 1000.0D;
            double localWLatency = localWriteLatency > 0.0D?localWriteLatency:0.0D / 0.0;
            statsTable.localWriteLatencyMs = localWLatency;
            statsTable.pendingFlushes = probe.getColumnFamilyMetric(keyspaceName, tableName, "PendingFlushes");
            statsTable.bloomFilterFalsePositives = probe.getColumnFamilyMetric(keyspaceName, tableName, "BloomFilterFalsePositives");
            statsTable.bloomFilterFalseRatio = probe.getColumnFamilyMetric(keyspaceName, tableName, "RecentBloomFilterFalseRatio");
            statsTable.bloomFilterSpaceUsed = this.format(((Long)probe.getColumnFamilyMetric(keyspaceName, tableName, "BloomFilterDiskSpaceUsed")).longValue(), humanReadable);
            if(bloomFilterOffHeapSize != null) {
               statsTable.bloomFilterOffHeapUsed = true;
               statsTable.bloomFilterOffHeapMemoryUsed = this.format(bloomFilterOffHeapSize.longValue(), humanReadable);
            }

            if(indexSummaryOffHeapSize != null) {
               statsTable.indexSummaryOffHeapUsed = true;
               statsTable.indexSummaryOffHeapMemoryUsed = this.format(indexSummaryOffHeapSize.longValue(), humanReadable);
            }

            if(compressionMetadataOffHeapSize != null) {
               statsTable.compressionMetadataOffHeapUsed = true;
               statsTable.compressionMetadataOffHeapMemoryUsed = this.format(compressionMetadataOffHeapSize.longValue(), humanReadable);
            }

            statsTable.compactedPartitionMinimumBytes = ((Long)probe.getColumnFamilyMetric(keyspaceName, tableName, "MinPartitionSize")).longValue();
            statsTable.compactedPartitionMaximumBytes = ((Long)probe.getColumnFamilyMetric(keyspaceName, tableName, "MaxPartitionSize")).longValue();
            statsTable.compactedPartitionMeanBytes = ((Long)probe.getColumnFamilyMetric(keyspaceName, tableName, "MeanPartitionSize")).longValue();
            CassandraMetricsRegistry.JmxHistogramMBean histogram = (CassandraMetricsRegistry.JmxHistogramMBean)probe.getColumnFamilyMetric(keyspaceName, tableName, "LiveScannedHistogram");
            statsTable.averageLiveCellsPerSliceLastFiveMinutes = histogram.getMean();
            statsTable.maximumLiveCellsPerSliceLastFiveMinutes = histogram.getMax();
            histogram = (CassandraMetricsRegistry.JmxHistogramMBean)probe.getColumnFamilyMetric(keyspaceName, tableName, "TombstoneScannedHistogram");
            statsTable.averageTombstonesPerSliceLastFiveMinutes = histogram.getMean();
            statsTable.maximumTombstonesPerSliceLastFiveMinutes = histogram.getMax();
            statsTable.droppedMutations = this.format(((Long)probe.getColumnFamilyMetric(keyspaceName, tableName, "DroppedMutations")).longValue(), humanReadable);
            statsKeyspace.tables.add(statsTable);
         }

         this.keyspaces.add(statsKeyspace);
      }

   }

   public static void fillSStablesPerLevel(int[] leveledSStables, int levelFanoutSize, List<String> sstablesInEachLevel) {
      for(int level = 0; level < leveledSStables.length; ++level) {
         int count = leveledSStables[level];
         long maxCount = 4L;
         if(level > 0) {
            maxCount = (long)Math.pow((double)levelFanoutSize, (double)level);
         }

         sstablesInEachLevel.add(count + ((long)count > maxCount?"/" + maxCount:""));
      }

   }

   private String format(long bytes, boolean humanReadable) {
      return humanReadable?FileUtils.stringifyFileSize((double)bytes):Long.toString(bytes);
   }

   private static class OptionFilter {
      private final Map<String, List<String>> filter = new HashMap();
      private final Map<String, List<String>> verifier = new HashMap();
      private final List<String> filterList = new ArrayList();
      private final boolean ignoreMode;

      OptionFilter(boolean ignoreMode, List<String> filterList) {
         this.filterList.addAll(filterList);
         this.ignoreMode = ignoreMode;
         Iterator var3 = filterList.iterator();

         while(var3.hasNext()) {
            String s = (String)var3.next();
            String[] keyValues = s.split("\\.", 2);
            if(!this.filter.containsKey(keyValues[0])) {
               this.filter.put(keyValues[0], new ArrayList());
               this.verifier.put(keyValues[0], new ArrayList());
            }

            if(keyValues.length == 2) {
               ((List)this.filter.get(keyValues[0])).add(keyValues[1]);
               ((List)this.verifier.get(keyValues[0])).add(keyValues[1]);
            }
         }

      }

      public boolean isTableIncluded(String keyspace, String table) {
         if(this.filterList.isEmpty()) {
            return !this.ignoreMode;
         } else {
            List<String> tables = (List)this.filter.get(keyspace);
            if(tables == null) {
               return this.ignoreMode;
            } else if(tables.isEmpty()) {
               return !this.ignoreMode;
            } else {
               ((List)this.verifier.get(keyspace)).remove(table);
               return this.ignoreMode ^ tables.contains(table);
            }
         }
      }

      public boolean isKeyspaceIncluded(String keyspace) {
         return this.filterList.isEmpty()?!this.ignoreMode:this.filter.get(keyspace) != null || this.ignoreMode;
      }

      public void verifyKeyspaces(List<String> keyspaces) {
         Iterator var2 = this.verifier.keySet().iterator();

         String ks;
         do {
            if(!var2.hasNext()) {
               return;
            }

            ks = (String)var2.next();
         } while(keyspaces.contains(ks));

         throw new IllegalArgumentException("Unknown keyspace: " + ks);
      }

      public void verifyTables() {
         Iterator var1 = this.filter.keySet().iterator();

         String ks;
         do {
            if(!var1.hasNext()) {
               return;
            }

            ks = (String)var1.next();
         } while(((List)this.verifier.get(ks)).isEmpty());

         throw new IllegalArgumentException("Unknown tables: " + this.verifier.get(ks) + " in keyspace: " + ks);
      }
   }
}
