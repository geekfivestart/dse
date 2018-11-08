package com.datastax.bdp.reporting.snapshots.db;

import com.datastax.bdp.reporting.CqlWritable;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import org.apache.cassandra.cql3.UntypedResultSet.Row;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.utils.ByteBufferUtil;

public class TableInfo implements CqlWritable {
   public final String ksName;
   public final String name;
   public final long totalReads;
   public final long totalWrites;
   public final long liveSSTableCount;
   public final long bfFalsePositives;
   public final long memtableSize;
   public final long memtableColumnsCount;
   public final long memtableSwitchCount;
   public final long unleveledSSTables;
   public final long minRowSize;
   public final long maxRowSize;
   public final long meanRowSize;
   public final long totalDataSize;
   public final double meanReadLatency;
   public final double meanWriteLatency;
   public final double bfFalsePositiveRatio;
   public final double keyCacheHitRate;
   public final double compressionRatio;
   public final double droppableTombstoneRatio;

   private TableInfo(String ksName, String name, long totalReads, long totalWrites, long liveSSTableCount, long bfFalsePositives, long memtableSize, long memtableColumnsCount, long memtableSwitchCount, long unleveledSSTables, long minRowSize, long maxRowSize, long meanRowSize, long totalDataSize, double meanReadLatency, double meanWriteLatency, double bfFalsePositiveRatio, double keyCacheHitRate, double compressionRatio, double droppableTombstoneRatio) {
      this.ksName = ksName;
      this.name = name;
      this.totalReads = totalReads;
      this.totalWrites = totalWrites;
      this.liveSSTableCount = liveSSTableCount;
      this.bfFalsePositives = bfFalsePositives;
      this.memtableSize = memtableSize;
      this.memtableColumnsCount = memtableColumnsCount;
      this.memtableSwitchCount = memtableSwitchCount;
      this.unleveledSSTables = unleveledSSTables;
      this.minRowSize = minRowSize;
      this.maxRowSize = maxRowSize;
      this.meanRowSize = meanRowSize;
      this.totalDataSize = totalDataSize;
      this.meanReadLatency = meanReadLatency;
      this.meanWriteLatency = meanWriteLatency;
      this.bfFalsePositiveRatio = bfFalsePositiveRatio;
      this.keyCacheHitRate = keyCacheHitRate;
      this.compressionRatio = compressionRatio;
      this.droppableTombstoneRatio = droppableTombstoneRatio;
   }

   public List<ByteBuffer> toByteBufferList() {
      List<ByteBuffer> vars = new LinkedList();
      vars.add(ByteBufferUtil.bytes(this.ksName));
      vars.add(ByteBufferUtil.bytes(this.name));
      vars.add(ByteBufferUtil.bytes(this.totalReads));
      vars.add(ByteBufferUtil.bytes(this.totalWrites));
      vars.add(ByteBufferUtil.bytes(this.meanReadLatency));
      vars.add(ByteBufferUtil.bytes(this.meanWriteLatency));
      vars.add(ByteBufferUtil.bytes(this.liveSSTableCount));
      vars.add(ByteBufferUtil.bytes(this.bfFalsePositives));
      vars.add(ByteBufferUtil.bytes(this.bfFalsePositiveRatio));
      vars.add(ByteBufferUtil.bytes(this.keyCacheHitRate));
      vars.add(ByteBufferUtil.bytes(this.compressionRatio));
      vars.add(ByteBufferUtil.bytes(this.droppableTombstoneRatio));
      vars.add(ByteBufferUtil.bytes(this.memtableSize));
      vars.add(ByteBufferUtil.bytes(this.memtableColumnsCount));
      vars.add(ByteBufferUtil.bytes(this.memtableSwitchCount));
      vars.add(ByteBufferUtil.bytes(this.unleveledSSTables));
      vars.add(ByteBufferUtil.bytes(this.minRowSize));
      vars.add(ByteBufferUtil.bytes(this.maxRowSize));
      vars.add(ByteBufferUtil.bytes(this.meanRowSize));
      vars.add(ByteBufferUtil.bytes(this.totalDataSize));
      return vars;
   }

   public static TableInfo fromRow(Row row) {
      return new TableInfo(row.getString("keyspace_name"), row.getString("table_name"), row.getLong("total_reads"), row.getLong("total_writes"), row.getLong("live_sstable_count"), row.getLong("bf_false_positives"), row.getLong("memtable_size"), row.getLong("memtable_columns_count"), row.getLong("memtable_switch_count"), row.getLong("unleveled_sstables"), row.getLong("min_row_size"), row.getLong("max_row_size"), row.getLong("mean_row_size"), row.getLong("total_data_size"), row.getDouble("mean_read_latency"), row.getDouble("mean_write_latency"), row.getDouble("bf_false_positive_ratio"), row.getDouble("key_cache_hit_rate"), row.getDouble("compression_ratio"), row.getDouble("droppable_tombstone_ratio"));
   }

   public static TableInfo fromColumnFamilyStore(ColumnFamilyStore cfs) {
      return new TableInfo(cfs.keyspace.getName(), cfs.getColumnFamilyName(), cfs.metric.readLatency.latency.getCount(), cfs.metric.writeLatency.latency.getCount(), (long)((Integer)cfs.metric.liveSSTableCount.getValue()).intValue(), ((Long)cfs.metric.bloomFilterFalsePositives.getValue()).longValue(), ((Long)cfs.metric.allMemtablesLiveDataSize.getValue()).longValue(), ((Long)cfs.metric.memtableColumnsCount.getValue()).longValue(), cfs.metric.memtableSwitchCount.getCount(), (long)cfs.getUnleveledSSTables(), ((Long)cfs.metric.minPartitionSize.getValue()).longValue(), ((Long)cfs.metric.maxPartitionSize.getValue()).longValue(), ((Long)cfs.metric.meanPartitionSize.getValue()).longValue(), cfs.metric.totalDiskSpaceUsed.getCount(), cfs.metric.readLatency.latency.getMeanRate(), cfs.metric.writeLatency.latency.getMeanRate(), ((Double)cfs.metric.bloomFilterFalseRatio.getValue()).doubleValue(), ((Double)cfs.metric.keyCacheHitRate.getValue()).doubleValue(), ((Double)cfs.metric.compressionRatio.getValue()).doubleValue(), cfs.getDroppableTombstoneRatio());
   }

   public static TableInfo aggregate(TableInfo t1, TableInfo t2) {
      assert t1.ksName.equals(t2.ksName);

      assert t1.name.equals(t2.name);

      return new TableInfo(t1.ksName, t1.name, t1.totalReads + t2.totalReads, t1.totalWrites + t2.totalWrites, t1.liveSSTableCount + t2.liveSSTableCount, t1.bfFalsePositives + t2.bfFalsePositives, t1.memtableSize + t2.memtableSize, t1.memtableColumnsCount + t2.memtableColumnsCount, t1.memtableSwitchCount + t2.memtableSwitchCount, t1.unleveledSSTables + t2.unleveledSSTables, Math.min(t1.minRowSize, t2.minRowSize), Math.max(t1.maxRowSize, t2.maxRowSize), (t1.meanRowSize + t2.meanRowSize) / 2L, t1.totalDataSize + t2.totalDataSize, addMeans(t1.meanReadLatency, t1.totalReads, t2.meanReadLatency, t2.totalReads), addMeans(t1.meanWriteLatency, t1.totalWrites, t2.meanWriteLatency, t2.totalWrites), (t1.bfFalsePositiveRatio + t2.bfFalsePositiveRatio) / 2.0D, (t1.keyCacheHitRate + t2.keyCacheHitRate) / 2.0D, (t1.compressionRatio + t2.compressionRatio) / 2.0D, (t1.droppableTombstoneRatio + t2.droppableTombstoneRatio) / 2.0D);
   }

   public static double addMeans(double mean1, long total1, double mean2, long total2) {
      return total1 + total2 == 0L?0.0D:(mean1 * (double)total1 + mean2 * (double)total2) / (double)(total1 + total2);
   }
}
