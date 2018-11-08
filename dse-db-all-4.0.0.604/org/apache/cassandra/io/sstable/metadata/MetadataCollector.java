package org.apache.cassandra.io.sstable.metadata;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.partitions.PartitionStatisticsCollector;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.MurmurHash;
import org.apache.cassandra.utils.streamhist.StreamingTombstoneHistogramBuilder;
import org.apache.cassandra.utils.streamhist.TombstoneHistogram;

public class MetadataCollector implements PartitionStatisticsCollector {
   public static final double NO_COMPRESSION_RATIO = -1.0D;
   protected EstimatedHistogram estimatedPartitionSize;
   protected EstimatedHistogram estimatedCellPerPartitionCount;
   protected IntervalSet<CommitLogPosition> commitLogIntervals;
   protected final MetadataCollector.MinMaxLongTracker timestampTracker;
   protected final MetadataCollector.MinMaxIntTracker localDeletionTimeTracker;
   protected final MetadataCollector.MinMaxIntTracker ttlTracker;
   protected double compressionRatio;
   protected StreamingTombstoneHistogramBuilder estimatedTombstoneDropTime;
   protected int sstableLevel;
   protected ByteBuffer[] minClusteringValues;
   protected ByteBuffer[] maxClusteringValues;
   protected boolean hasLegacyCounterShards;
   protected long totalColumnsSet;
   protected long totalRows;
   private long currentPartitionCells;
   protected ICardinality cardinality;
   private final ClusteringComparator comparator;

   static EstimatedHistogram defaultCellPerPartitionCountHistogram() {
      return new EstimatedHistogram(114);
   }

   static EstimatedHistogram defaultPartitionSizeHistogram() {
      return new EstimatedHistogram(150);
   }

   static TombstoneHistogram defaultTombstoneDropTimeHistogram() {
      return TombstoneHistogram.createDefault();
   }

   public static StatsMetadata defaultStatsMetadata() {
      return new StatsMetadata(defaultPartitionSizeHistogram(), defaultCellPerPartitionCountHistogram(), IntervalSet.empty(), -9223372036854775808L, 9223372036854775807L, 2147483647, 2147483647, 0, 2147483647, -1.0D, defaultTombstoneDropTimeHistogram(), 0, Collections.emptyList(), Collections.emptyList(), true, 0L, -1L, -1L, (UUID)null);
   }

   public MetadataCollector(ClusteringComparator comparator) {
      this.estimatedPartitionSize = defaultPartitionSizeHistogram();
      this.estimatedCellPerPartitionCount = defaultCellPerPartitionCountHistogram();
      this.commitLogIntervals = IntervalSet.empty();
      this.timestampTracker = new MetadataCollector.MinMaxLongTracker();
      this.localDeletionTimeTracker = new MetadataCollector.MinMaxIntTracker(2147483647, 2147483647);
      this.ttlTracker = new MetadataCollector.MinMaxIntTracker(0, 0);
      this.compressionRatio = -1.0D;
      this.estimatedTombstoneDropTime = new StreamingTombstoneHistogramBuilder(100, 100000, SSTable.TOMBSTONE_HISTOGRAM_TTL_ROUND_SECONDS);
      this.hasLegacyCounterShards = false;
      this.currentPartitionCells = 0L;
      this.cardinality = new HyperLogLogPlus(13, 25);
      this.comparator = comparator;
      this.minClusteringValues = new ByteBuffer[comparator.size()];
      this.maxClusteringValues = new ByteBuffer[comparator.size()];
   }

   public MetadataCollector(Iterable<SSTableReader> sstables, ClusteringComparator comparator, int level) {
      this(comparator);
      IntervalSet.Builder<CommitLogPosition> intervals = new IntervalSet.Builder();
      Iterator var5 = sstables.iterator();

      while(var5.hasNext()) {
         SSTableReader sstable = (SSTableReader)var5.next();
         intervals.addAll(sstable.getSSTableMetadata().commitLogIntervals);
      }

      this.commitLogIntervals(intervals.build());
      this.sstableLevel(level);
   }

   public MetadataCollector copy() {
      return (new MetadataCollector(this.comparator)).commitLogIntervals(this.commitLogIntervals);
   }

   public MetadataCollector addKey(ByteBuffer key) {
      long hashed = MurmurHash.hash2_64(key, key.position(), key.remaining(), 0L);
      this.cardinality.offerHashed(hashed);
      return this;
   }

   public MetadataCollector addPartitionSizeInBytes(long partitionSize) {
      this.estimatedPartitionSize.add(partitionSize);
      return this;
   }

   public MetadataCollector addCellPerPartitionCount() {
      this.estimatedCellPerPartitionCount.add(this.currentPartitionCells);
      this.currentPartitionCells = 0L;
      return this;
   }

   public MetadataCollector addCompressionRatio(long compressed, long uncompressed) {
      this.compressionRatio = (double)compressed / (double)uncompressed;
      return this;
   }

   public void update(LivenessInfo newInfo) {
      if(!newInfo.isEmpty()) {
         this.updateTimestamp(newInfo.timestamp());
         this.updateTTL(newInfo.ttl());
         this.updateLocalDeletionTime(newInfo.localExpirationTime());
      }
   }

   public void update(Cell cell) {
      ++this.currentPartitionCells;
      this.updateTimestamp(cell.timestamp());
      this.updateTTL(cell.ttl());
      this.updateLocalDeletionTime(cell.localDeletionTime());
   }

   public void update(ColumnData cd) {
      ++this.totalColumnsSet;
   }

   public void update(DeletionTime dt) {
      if(!dt.isLive()) {
         this.updateTimestamp(dt.markedForDeleteAt());
         this.updateLocalDeletionTime(dt.localDeletionTime());
      }

   }

   public void updateRowStats() {
      ++this.totalRows;
   }

   private void updateTimestamp(long newTimestamp) {
      this.timestampTracker.update(newTimestamp);
   }

   private void updateLocalDeletionTime(int newLocalDeletionTime) {
      this.localDeletionTimeTracker.update(newLocalDeletionTime);
      if(newLocalDeletionTime != 2147483647) {
         this.estimatedTombstoneDropTime.update(newLocalDeletionTime);
      }

   }

   private void updateTTL(int newTTL) {
      this.ttlTracker.update(newTTL);
   }

   public MetadataCollector commitLogIntervals(IntervalSet<CommitLogPosition> commitLogIntervals) {
      this.commitLogIntervals = commitLogIntervals;
      return this;
   }

   public MetadataCollector sstableLevel(int sstableLevel) {
      this.sstableLevel = sstableLevel;
      return this;
   }

   public MetadataCollector updateClusteringValues(ClusteringPrefix clustering) {
      int size = clustering.size();

      for(int i = 0; i < size; ++i) {
         AbstractType<?> type = this.comparator.subtype(i);
         ByteBuffer newValue = clustering.get(i);
         this.minClusteringValues[i] = maybeMinimize(min(this.minClusteringValues[i], newValue, type));
         this.maxClusteringValues[i] = maybeMinimize(max(this.maxClusteringValues[i], newValue, type));
      }

      return this;
   }

   private static ByteBuffer maybeMinimize(ByteBuffer buffer) {
      return buffer == null?null:ByteBufferUtil.minimalBufferFor(buffer);
   }

   private static ByteBuffer min(ByteBuffer b1, ByteBuffer b2, AbstractType<?> comparator) {
      return b1 == null?b2:(b2 == null?b1:(comparator.compare(b1, b2) >= 0?b2:b1));
   }

   private static ByteBuffer max(ByteBuffer b1, ByteBuffer b2, AbstractType<?> comparator) {
      return b1 == null?b2:(b2 == null?b1:(comparator.compare(b1, b2) >= 0?b1:b2));
   }

   public void updateHasLegacyCounterShards(boolean hasLegacyCounterShards) {
      this.hasLegacyCounterShards = this.hasLegacyCounterShards || hasLegacyCounterShards;
   }

   public Map<MetadataType, MetadataComponent> finalizeMetadata(String partitioner, double bloomFilterFPChance, long repairedAt, UUID pendingRepair, SerializationHeader header) {
      Map<MetadataType, MetadataComponent> components = new EnumMap(MetadataType.class);
      components.put(MetadataType.VALIDATION, new ValidationMetadata(partitioner, bloomFilterFPChance));
      components.put(MetadataType.STATS, new StatsMetadata(this.estimatedPartitionSize, this.estimatedCellPerPartitionCount, this.commitLogIntervals, this.timestampTracker.min(), this.timestampTracker.max(), this.localDeletionTimeTracker.min(), this.localDeletionTimeTracker.max(), this.ttlTracker.min(), this.ttlTracker.max(), this.compressionRatio, this.estimatedTombstoneDropTime.build(), this.sstableLevel, makeList(this.minClusteringValues), makeList(this.maxClusteringValues), this.hasLegacyCounterShards, repairedAt, this.totalColumnsSet, this.totalRows, pendingRepair));
      components.put(MetadataType.COMPACTION, new CompactionMetadata(this.cardinality));
      components.put(MetadataType.HEADER, header.toComponent());
      return components;
   }

   private static List<ByteBuffer> makeList(ByteBuffer[] values) {
      List<ByteBuffer> l = new ArrayList(values.length);

      for(int i = 0; i < values.length && values[i] != null; ++i) {
         l.add(values[i]);
      }

      return l;
   }

   public static class MinMaxIntTracker {
      private final int defaultMin;
      private final int defaultMax;
      private boolean isSet;
      private int min;
      private int max;

      public MinMaxIntTracker() {
         this(-2147483648, 2147483647);
      }

      public MinMaxIntTracker(int defaultMin, int defaultMax) {
         this.isSet = false;
         this.defaultMin = defaultMin;
         this.defaultMax = defaultMax;
      }

      public void update(int value) {
         if(!this.isSet) {
            this.min = this.max = value;
            this.isSet = true;
         } else {
            if(value < this.min) {
               this.min = value;
            }

            if(value > this.max) {
               this.max = value;
            }
         }

      }

      public int min() {
         return this.isSet?this.min:this.defaultMin;
      }

      public int max() {
         return this.isSet?this.max:this.defaultMax;
      }
   }

   public static class MinMaxLongTracker {
      private final long defaultMin;
      private final long defaultMax;
      private boolean isSet;
      private long min;
      private long max;

      public MinMaxLongTracker() {
         this(-9223372036854775808L, 9223372036854775807L);
      }

      public MinMaxLongTracker(long defaultMin, long defaultMax) {
         this.isSet = false;
         this.defaultMin = defaultMin;
         this.defaultMax = defaultMax;
      }

      public void update(long value) {
         if(!this.isSet) {
            this.min = this.max = value;
            this.isSet = true;
         } else {
            if(value < this.min) {
               this.min = value;
            }

            if(value > this.max) {
               this.max = value;
            }
         }

      }

      public long min() {
         return this.isSet?this.min:this.defaultMin;
      }

      public long max() {
         return this.isSet?this.max:this.defaultMax;
      }
   }
}
