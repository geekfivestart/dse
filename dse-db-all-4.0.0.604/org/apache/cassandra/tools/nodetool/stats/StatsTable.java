package org.apache.cassandra.tools.nodetool.stats;

import java.util.ArrayList;
import java.util.List;

public class StatsTable {
   public String name;
   public boolean isIndex;
   public boolean isLeveledSstable = false;
   public Object sstableCount;
   public String spaceUsedLive;
   public String spaceUsedTotal;
   public String spaceUsedBySnapshotsTotal;
   public boolean offHeapUsed = false;
   public String offHeapMemoryUsedTotal;
   public Object sstableCompressionRatio;
   public Object numberOfPartitionsEstimate;
   public Object memtableCellCount;
   public String memtableDataSize;
   public boolean memtableOffHeapUsed = false;
   public String memtableOffHeapMemoryUsed;
   public Object memtableSwitchCount;
   public long localReadCount;
   public double localReadLatencyMs;
   public long localWriteCount;
   public double localWriteLatencyMs;
   public Object pendingFlushes;
   public Object bloomFilterFalsePositives;
   public Object bloomFilterFalseRatio;
   public String bloomFilterSpaceUsed;
   public boolean bloomFilterOffHeapUsed = false;
   public String bloomFilterOffHeapMemoryUsed;
   public boolean indexSummaryOffHeapUsed = false;
   public String indexSummaryOffHeapMemoryUsed;
   public boolean compressionMetadataOffHeapUsed = false;
   public String compressionMetadataOffHeapMemoryUsed;
   public long compactedPartitionMinimumBytes;
   public long compactedPartitionMaximumBytes;
   public long compactedPartitionMeanBytes;
   public double percentRepaired;
   public long bytesRepaired;
   public long bytesUnrepaired;
   public long bytesPendingRepair;
   public double averageLiveCellsPerSliceLastFiveMinutes;
   public long maximumLiveCellsPerSliceLastFiveMinutes;
   public double averageTombstonesPerSliceLastFiveMinutes;
   public long maximumTombstonesPerSliceLastFiveMinutes;
   public String droppedMutations;
   public String failedReplicationCount;
   public List<String> sstablesInEachLevel = new ArrayList();

   public StatsTable() {
   }
}
