package org.apache.cassandra.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.RatioGauge;
import com.codahale.metrics.RatioGauge.Ratio;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.compaction.CompactionStrategyManager;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.TopKSampler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableMetrics {
   private static final Logger logger = LoggerFactory.getLogger(TableMetrics.class);
   public static final long[] EMPTY = new long[0];
   public final Gauge<Long> memtableOnHeapSize;
   public final Gauge<Long> memtableOffHeapSize;
   public final Gauge<Long> memtableLiveDataSize;
   public final Gauge<Long> allMemtablesOnHeapSize;
   public final Gauge<Long> allMemtablesOffHeapSize;
   public final Gauge<Long> allMemtablesLiveDataSize;
   public final Gauge<Long> memtableColumnsCount;
   public final Counter memtableSwitchCount;
   public final Gauge<Double> compressionRatio;
   public final Gauge<long[]> estimatedPartitionSizeHistogram;
   public final Gauge<Long> estimatedPartitionCount;
   public final Gauge<long[]> estimatedColumnCountHistogram;
   public final Histogram sstablesPerReadHistogram;
   public final LatencyMetrics readLatency;
   public final LatencyMetrics rangeLatency;
   public final LatencyMetrics writeLatency;
   public final Counter pendingFlushes;
   public final Counter bytesFlushed;
   public final Counter compactionBytesWritten;
   public final Gauge<Integer> pendingCompactions;
   public final Gauge<Integer> liveSSTableCount;
   public final Counter liveDiskSpaceUsed;
   public final Counter totalDiskSpaceUsed;
   public final Gauge<Long> minPartitionSize;
   public final Gauge<Long> maxPartitionSize;
   public final Gauge<Long> meanPartitionSize;
   public final Gauge<Long> bloomFilterFalsePositives;
   public final Gauge<Long> recentBloomFilterFalsePositives;
   public final Gauge<Double> bloomFilterFalseRatio;
   public final Gauge<Double> recentBloomFilterFalseRatio;
   public final Gauge<Long> bloomFilterDiskSpaceUsed;
   public final Gauge<Long> bloomFilterOffHeapMemoryUsed;
   public final Gauge<Long> indexSummaryOffHeapMemoryUsed;
   public final Gauge<Long> compressionMetadataOffHeapMemoryUsed;
   public final Gauge<Double> keyCacheHitRate;
   public final Histogram tombstoneScannedHistogram;
   public final Histogram liveScannedHistogram;
   public final Histogram colUpdateTimeDeltaHistogram;
   public final Timer viewLockAcquireTime;
   public final Counter viewLockAcquisitionTimeouts;
   public final Timer viewReadTime;
   public final Gauge<Long> trueSnapshotsSize;
   public final Counter rowCacheHitOutOfRange;
   public final Counter rowCacheHit;
   public final Counter rowCacheMiss;
   public final Counter tombstoneFailures;
   public final Counter tombstoneWarnings;
   public final LatencyMetrics casPrepare;
   public final LatencyMetrics casPropose;
   public final LatencyMetrics casCommit;
   public final Gauge<Double> percentRepaired;
   public final Gauge<Long> bytesRepaired;
   public final Gauge<Long> bytesUnrepaired;
   public final Gauge<Long> bytesPendingRepair;
   public final Counter repairsStarted;
   public final Counter repairsCompleted;
   public final Timer anticompactionTime;
   public final Timer validationTime;
   public final Timer syncTime;
   public final Histogram bytesValidated;
   public final Histogram partitionsValidated;
   public final Counter bytesAnticompacted;
   public final Counter bytesMutatedAnticompaction;
   public final Gauge<Double> mutatedAnticompactionGauge;
   public final Timer coordinatorReadLatency;
   public final Timer coordinatorScanLatency;
   public final Histogram waitingOnFreeMemtableSpace;
   public final Counter droppedMutations;
   public final NodeSyncMetrics nodeSyncMetrics;
   private final MetricNameFactory factory;
   private final MetricNameFactory aliasFactory;
   private static final MetricNameFactory globalFactory = new TableMetrics.AllTableMetricNameFactory("Table");
   private static final MetricNameFactory globalAliasFactory = new TableMetrics.AllTableMetricNameFactory("ColumnFamily");
   public final Counter speculativeRetries;
   public final Counter speculativeFailedRetries;
   public final Counter speculativeInsufficientReplicas;
   public final Gauge<Long> speculativeSampleLatencyNanos;
   public static final LatencyMetrics globalReadLatency;
   public static final LatencyMetrics globalWriteLatency;
   public static final LatencyMetrics globalRangeLatency;
   public static final Gauge<Double> globalPercentRepaired;
   public static final Gauge<Long> globalBytesRepaired;
   public static final Gauge<Long> globalBytesUnrepaired;
   public static final Gauge<Long> globalBytesPendingRepair;
   public final Meter readRepairRequests;
   public final Meter shortReadProtectionRequests;
   public final Map<TableMetrics.Sampler, TopKSampler<ByteBuffer>> samplers;
   public static final ConcurrentMap<String, Set<Metric>> allTableMetrics;
   public static final ConcurrentMap<String, String> all;

   private static Pair<Long, Long> totalNonSystemTablesSize(Predicate<SSTableReader> predicate) {
      long total = 0L;
      long filtered = 0L;
      Iterator var5 = Keyspace.nonSystem().iterator();

      label47:
      while(true) {
         Keyspace k;
         do {
            do {
               if(!var5.hasNext()) {
                  return Pair.create(Long.valueOf(filtered), Long.valueOf(total));
               }

               k = (Keyspace)var5.next();
            } while("system_distributed".equals(k.getName()));
         } while(k.getReplicationStrategy().getReplicationFactor() < 2);

         Iterator var7 = k.getColumnFamilyStores().iterator();

         while(true) {
            ColumnFamilyStore cf;
            do {
               if(!var7.hasNext()) {
                  continue label47;
               }

               cf = (ColumnFamilyStore)var7.next();
            } while(SecondaryIndexManager.isIndexColumnFamily(cf.name));

            SSTableReader sstable;
            for(Iterator var9 = cf.getSSTables(SSTableSet.CANONICAL).iterator(); var9.hasNext(); total += sstable.uncompressedLength()) {
               sstable = (SSTableReader)var9.next();
               if(predicate.test(sstable)) {
                  filtered += sstable.uncompressedLength();
               }
            }
         }
      }
   }

   private static long[] combineHistograms(Iterable<SSTableReader> sstables, TableMetrics.GetHistogram getHistogram) {
      Iterator<SSTableReader> iterator = sstables.iterator();
      if(!iterator.hasNext()) {
         return EMPTY;
      } else {
         long[] firstBucket = getHistogram.getHistogram((SSTableReader)iterator.next()).getBuckets(false);
         long[] values = Arrays.copyOf(firstBucket, firstBucket.length);

         while(true) {
            while(iterator.hasNext()) {
               long[] nextBucket = getHistogram.getHistogram((SSTableReader)iterator.next()).getBuckets(false);
               if(nextBucket.length > values.length) {
                  long[] newValues = Arrays.copyOf(firstBucket, nextBucket.length);

                  for(int i = 0; i < newValues.length; ++i) {
                     newValues[i] += nextBucket[i];
                  }

                  values = newValues;
               } else {
                  for(int i = 0; i < values.length; ++i) {
                     values[i] += nextBucket[i];
                  }
               }
            }

            return values;
         }
      }
   }

   public TableMetrics(final ColumnFamilyStore cfs) {
      this.factory = new TableMetrics.TableMetricNameFactory(cfs, "Table");
      this.aliasFactory = new TableMetrics.TableMetricNameFactory(cfs, "ColumnFamily");
      this.samplers = new EnumMap(TableMetrics.Sampler.class);
      TableMetrics.Sampler[] var2 = TableMetrics.Sampler.values();
      int var3 = var2.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         TableMetrics.Sampler sampler = var2[var4];
         this.samplers.put(sampler, new TopKSampler());
      }

      this.memtableColumnsCount = this.createTableGauge("MemtableColumnsCount", new Gauge<Long>() {
         public Long getValue() {
            return Long.valueOf(cfs.getTracker().getView().getCurrentMemtable().getOperations());
         }
      });
      this.memtableOnHeapSize = this.createTableGauge("MemtableOnHeapSize", new Gauge<Long>() {
         public Long getValue() {
            return Long.valueOf(cfs.getTracker().getView().getCurrentMemtable().getMemoryUsage().ownsOnHeap);
         }
      });
      this.memtableOffHeapSize = this.createTableGauge("MemtableOffHeapSize", new Gauge<Long>() {
         public Long getValue() {
            return Long.valueOf(cfs.getTracker().getView().getCurrentMemtable().getMemoryUsage().ownsOffHeap);
         }
      });
      this.memtableLiveDataSize = this.createTableGauge("MemtableLiveDataSize", new Gauge<Long>() {
         public Long getValue() {
            return Long.valueOf(cfs.getTracker().getView().getCurrentMemtable().getLiveDataSize());
         }
      });
      this.allMemtablesOnHeapSize = this.createTableGauge("AllMemtablesHeapSize", new Gauge<Long>() {
         public Long getValue() {
            return Long.valueOf(cfs.getMemoryUsageIncludingIndexes().ownsOnHeap);
         }
      });
      this.allMemtablesOffHeapSize = this.createTableGauge("AllMemtablesOffHeapSize", new Gauge<Long>() {
         public Long getValue() {
            return Long.valueOf(cfs.getMemoryUsageIncludingIndexes().ownsOffHeap);
         }
      });
      this.allMemtablesLiveDataSize = this.createTableGauge("AllMemtablesLiveDataSize", new Gauge<Long>() {
         public Long getValue() {
            long size = 0L;

            ColumnFamilyStore cfs2;
            for(Iterator var3 = cfs.concatWithIndexes().iterator(); var3.hasNext(); size += cfs2.getTracker().getView().getCurrentMemtable().getLiveDataSize()) {
               cfs2 = (ColumnFamilyStore)var3.next();
            }

            return Long.valueOf(size);
         }
      });
      this.memtableSwitchCount = this.createTableCounter("MemtableSwitchCount");
      this.estimatedPartitionSizeHistogram = (Gauge)CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName("EstimatedPartitionSizeHistogram"), this.aliasFactory.createMetricName("EstimatedRowSizeHistogram"), new Gauge<long[]>() {
         public long[] getValue() {
            return TableMetrics.combineHistograms(cfs.getSSTables(SSTableSet.CANONICAL), new TableMetrics.GetHistogram() {
               public EstimatedHistogram getHistogram(SSTableReader reader) {
                  return reader.getEstimatedPartitionSize();
               }
            });
         }
      });
      this.estimatedPartitionCount = (Gauge)CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName("EstimatedPartitionCount"), this.aliasFactory.createMetricName("EstimatedRowCount"), new Gauge<Long>() {
         public Long getValue() {
            long memtablePartitions = 0L;

            Memtable memtable;
            for(Iterator var3 = cfs.getTracker().getView().getAllMemtables().iterator(); var3.hasNext(); memtablePartitions += (long)memtable.partitionCount()) {
               memtable = (Memtable)var3.next();
            }

            return Long.valueOf(SSTableReader.getApproximateKeyCount(cfs.getSSTables(SSTableSet.CANONICAL)) + memtablePartitions);
         }
      });
      this.estimatedColumnCountHistogram = (Gauge)CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName("EstimatedColumnCountHistogram"), this.aliasFactory.createMetricName("EstimatedColumnCountHistogram"), new Gauge<long[]>() {
         public long[] getValue() {
            return TableMetrics.combineHistograms(cfs.getSSTables(SSTableSet.CANONICAL), new TableMetrics.GetHistogram() {
               public EstimatedHistogram getHistogram(SSTableReader reader) {
                  return reader.getEstimatedColumnCount();
               }
            });
         }
      });
      this.sstablesPerReadHistogram = this.createTableHistogram("SSTablesPerReadHistogram", cfs.keyspace.metric.sstablesPerReadHistogram);
      this.compressionRatio = this.createTableGauge("CompressionRatio", new Gauge<Double>() {
         public Double getValue() {
            return TableMetrics.computeCompressionRatio(cfs.getSSTables(SSTableSet.CANONICAL));
         }
      }, new Gauge<Double>() {
         public Double getValue() {
            List<SSTableReader> sstables = new ArrayList();
            Keyspace.all().forEach((ks) -> {
               sstables.addAll(ks.getAllSSTables(SSTableSet.CANONICAL));
            });
            return TableMetrics.computeCompressionRatio(sstables);
         }
      });
      this.percentRepaired = this.createTableGauge("PercentRepaired", new Gauge<Double>() {
         public Double getValue() {
            double repaired = 0.0D;
            double total = 0.0D;

            SSTableReader sstable;
            for(Iterator var5 = cfs.getSSTables(SSTableSet.CANONICAL).iterator(); var5.hasNext(); total += (double)sstable.uncompressedLength()) {
               sstable = (SSTableReader)var5.next();
               if(sstable.isRepaired()) {
                  repaired += (double)sstable.uncompressedLength();
               }
            }

            return Double.valueOf(total > 0.0D?repaired / total * 100.0D:100.0D);
         }
      });
      this.bytesRepaired = this.createTableGauge("BytesRepaired", new Gauge<Long>() {
         public Long getValue() {
            long size = 0L;

            SSTableReader sstable;
            for(Iterator var3 = Iterables.filter(cfs.getSSTables(SSTableSet.CANONICAL), SSTableReader::isRepaired).iterator(); var3.hasNext(); size += sstable.uncompressedLength()) {
               sstable = (SSTableReader)var3.next();
            }

            return Long.valueOf(size);
         }
      });
      this.bytesUnrepaired = this.createTableGauge("BytesUnrepaired", new Gauge<Long>() {
         public Long getValue() {
            long size = 0L;

            SSTableReader sstable;
            for(Iterator var3 = Iterables.filter(cfs.getSSTables(SSTableSet.CANONICAL), (s) -> {
               return !s.isRepaired() && !s.isPendingRepair();
            }).iterator(); var3.hasNext(); size += sstable.uncompressedLength()) {
               sstable = (SSTableReader)var3.next();
            }

            return Long.valueOf(size);
         }
      });
      this.bytesPendingRepair = this.createTableGauge("BytesPendingRepair", new Gauge<Long>() {
         public Long getValue() {
            long size = 0L;

            SSTableReader sstable;
            for(Iterator var3 = Iterables.filter(cfs.getSSTables(SSTableSet.CANONICAL), SSTableReader::isPendingRepair).iterator(); var3.hasNext(); size += sstable.uncompressedLength()) {
               sstable = (SSTableReader)var3.next();
            }

            return Long.valueOf(size);
         }
      });
      this.readLatency = new LatencyMetrics(this.factory, this.aliasFactory, "Read", new LatencyMetrics[]{cfs.keyspace.metric.readLatency, globalReadLatency});
      this.writeLatency = new LatencyMetrics(this.factory, this.aliasFactory, "Write", new LatencyMetrics[]{cfs.keyspace.metric.writeLatency, globalWriteLatency});
      this.rangeLatency = new LatencyMetrics(this.factory, this.aliasFactory, "Range", new LatencyMetrics[]{cfs.keyspace.metric.rangeLatency, globalRangeLatency});
      this.pendingFlushes = this.createTableCounter("PendingFlushes");
      this.bytesFlushed = this.createTableCounter("BytesFlushed");
      this.compactionBytesWritten = this.createTableCounter("CompactionBytesWritten");
      this.pendingCompactions = this.createTableGauge("PendingCompactions", new Gauge<Integer>() {
         public Integer getValue() {
            CompactionStrategyManager compactionStrategyManager = cfs.getCompactionStrategyManager();
            return Integer.valueOf(compactionStrategyManager != null?compactionStrategyManager.getEstimatedRemainingTasks():0);
         }
      });
      this.liveSSTableCount = this.createTableGauge("LiveSSTableCount", new Gauge<Integer>() {
         public Integer getValue() {
            return Integer.valueOf(cfs.getTracker().getView().liveSSTables().size());
         }
      });
      this.liveDiskSpaceUsed = this.createTableCounter("LiveDiskSpaceUsed");
      this.totalDiskSpaceUsed = this.createTableCounter("TotalDiskSpaceUsed");
      this.minPartitionSize = this.createTableGauge("MinPartitionSize", "MinRowSize", new Gauge<Long>() {
         public Long getValue() {
            long min = 0L;
            Iterator var3 = cfs.getSSTables(SSTableSet.CANONICAL).iterator();

            while(true) {
               SSTableReader sstable;
               do {
                  if(!var3.hasNext()) {
                     return Long.valueOf(min);
                  }

                  sstable = (SSTableReader)var3.next();
               } while(min != 0L && sstable.getEstimatedPartitionSize().min() >= min);

               min = sstable.getEstimatedPartitionSize().min();
            }
         }
      }, new Gauge<Long>() {
         public Long getValue() {
            long min = 9223372036854775807L;

            Metric cfGauge;
            for(Iterator var3 = ((Set)TableMetrics.allTableMetrics.get("MinPartitionSize")).iterator(); var3.hasNext(); min = Math.min(min, ((Number)((Gauge)cfGauge).getValue()).longValue())) {
               cfGauge = (Metric)var3.next();
            }

            return Long.valueOf(min);
         }
      });
      this.maxPartitionSize = this.createTableGauge("MaxPartitionSize", "MaxRowSize", new Gauge<Long>() {
         public Long getValue() {
            long max = 0L;
            Iterator var3 = cfs.getSSTables(SSTableSet.CANONICAL).iterator();

            while(var3.hasNext()) {
               SSTableReader sstable = (SSTableReader)var3.next();
               if(sstable.getEstimatedPartitionSize().max() > max) {
                  max = sstable.getEstimatedPartitionSize().max();
               }
            }

            return Long.valueOf(max);
         }
      }, new Gauge<Long>() {
         public Long getValue() {
            long max = 0L;

            Metric cfGauge;
            for(Iterator var3 = ((Set)TableMetrics.allTableMetrics.get("MaxPartitionSize")).iterator(); var3.hasNext(); max = Math.max(max, ((Number)((Gauge)cfGauge).getValue()).longValue())) {
               cfGauge = (Metric)var3.next();
            }

            return Long.valueOf(max);
         }
      });
      this.meanPartitionSize = this.createTableGauge("MeanPartitionSize", "MeanRowSize", new Gauge<Long>() {
         public Long getValue() {
            long sum = 0L;
            long count = 0L;

            long n;
            for(Iterator var5 = cfs.getSSTables(SSTableSet.CANONICAL).iterator(); var5.hasNext(); count += n) {
               SSTableReader sstable = (SSTableReader)var5.next();
               n = sstable.getEstimatedPartitionSize().count();
               sum += sstable.getEstimatedPartitionSize().mean() * n;
            }

            return Long.valueOf(count > 0L?sum / count:0L);
         }
      }, new Gauge<Long>() {
         public Long getValue() {
            long sum = 0L;
            long count = 0L;
            Iterator var5 = Keyspace.all().iterator();

            while(var5.hasNext()) {
               Keyspace keyspace = (Keyspace)var5.next();

               long n;
               for(Iterator var7 = keyspace.getAllSSTables(SSTableSet.CANONICAL).iterator(); var7.hasNext(); count += n) {
                  SSTableReader sstable = (SSTableReader)var7.next();
                  n = sstable.getEstimatedPartitionSize().count();
                  sum += sstable.getEstimatedPartitionSize().mean() * n;
               }
            }

            return Long.valueOf(count > 0L?sum / count:0L);
         }
      });
      this.bloomFilterFalsePositives = this.createTableGauge("BloomFilterFalsePositives", new Gauge<Long>() {
         public Long getValue() {
            long count = 0L;

            SSTableReader sstable;
            for(Iterator var3 = cfs.getSSTables(SSTableSet.LIVE).iterator(); var3.hasNext(); count += sstable.getBloomFilterFalsePositiveCount()) {
               sstable = (SSTableReader)var3.next();
            }

            return Long.valueOf(count);
         }
      });
      this.recentBloomFilterFalsePositives = this.createTableGauge("RecentBloomFilterFalsePositives", new Gauge<Long>() {
         public Long getValue() {
            long count = 0L;

            SSTableReader sstable;
            for(Iterator var3 = cfs.getSSTables(SSTableSet.LIVE).iterator(); var3.hasNext(); count += sstable.getRecentBloomFilterFalsePositiveCount()) {
               sstable = (SSTableReader)var3.next();
            }

            return Long.valueOf(count);
         }
      });
      this.bloomFilterFalseRatio = this.createTableGauge("BloomFilterFalseRatio", new Gauge<Double>() {
         public Double getValue() {
            long falseCount = 0L;
            long trueCount = 0L;

            SSTableReader sstable;
            for(Iterator var5 = cfs.getSSTables(SSTableSet.LIVE).iterator(); var5.hasNext(); trueCount += sstable.getBloomFilterTruePositiveCount()) {
               sstable = (SSTableReader)var5.next();
               falseCount += sstable.getBloomFilterFalsePositiveCount();
            }

            return falseCount == 0L && trueCount == 0L?Double.valueOf(0.0D):Double.valueOf((double)falseCount / (double)(trueCount + falseCount));
         }
      }, new Gauge<Double>() {
         public Double getValue() {
            long falseCount = 0L;
            long trueCount = 0L;
            Iterator var5 = Keyspace.all().iterator();

            while(var5.hasNext()) {
               Keyspace keyspace = (Keyspace)var5.next();

               SSTableReader sstable;
               for(Iterator var7 = keyspace.getAllSSTables(SSTableSet.LIVE).iterator(); var7.hasNext(); trueCount += sstable.getBloomFilterTruePositiveCount()) {
                  sstable = (SSTableReader)var7.next();
                  falseCount += sstable.getBloomFilterFalsePositiveCount();
               }
            }

            if(falseCount == 0L && trueCount == 0L) {
               return Double.valueOf(0.0D);
            } else {
               return Double.valueOf((double)falseCount / (double)(trueCount + falseCount));
            }
         }
      });
      this.recentBloomFilterFalseRatio = this.createTableGauge("RecentBloomFilterFalseRatio", new Gauge<Double>() {
         public Double getValue() {
            long falseCount = 0L;
            long trueCount = 0L;

            SSTableReader sstable;
            for(Iterator var5 = cfs.getSSTables(SSTableSet.LIVE).iterator(); var5.hasNext(); trueCount += sstable.getRecentBloomFilterTruePositiveCount()) {
               sstable = (SSTableReader)var5.next();
               falseCount += sstable.getRecentBloomFilterFalsePositiveCount();
            }

            return falseCount == 0L && trueCount == 0L?Double.valueOf(0.0D):Double.valueOf((double)falseCount / (double)(trueCount + falseCount));
         }
      }, new Gauge<Double>() {
         public Double getValue() {
            long falseCount = 0L;
            long trueCount = 0L;
            Iterator var5 = Keyspace.all().iterator();

            while(var5.hasNext()) {
               Keyspace keyspace = (Keyspace)var5.next();

               SSTableReader sstable;
               for(Iterator var7 = keyspace.getAllSSTables(SSTableSet.LIVE).iterator(); var7.hasNext(); trueCount += sstable.getRecentBloomFilterTruePositiveCount()) {
                  sstable = (SSTableReader)var7.next();
                  falseCount += sstable.getRecentBloomFilterFalsePositiveCount();
               }
            }

            if(falseCount == 0L && trueCount == 0L) {
               return Double.valueOf(0.0D);
            } else {
               return Double.valueOf((double)falseCount / (double)(trueCount + falseCount));
            }
         }
      });
      this.bloomFilterDiskSpaceUsed = this.createTableGauge("BloomFilterDiskSpaceUsed", new Gauge<Long>() {
         public Long getValue() {
            long total = 0L;

            SSTableReader sst;
            for(Iterator var3 = cfs.getSSTables(SSTableSet.CANONICAL).iterator(); var3.hasNext(); total += sst.getBloomFilterSerializedSize()) {
               sst = (SSTableReader)var3.next();
            }

            return Long.valueOf(total);
         }
      });
      this.bloomFilterOffHeapMemoryUsed = this.createTableGauge("BloomFilterOffHeapMemoryUsed", new Gauge<Long>() {
         public Long getValue() {
            long total = 0L;

            SSTableReader sst;
            for(Iterator var3 = cfs.getSSTables(SSTableSet.LIVE).iterator(); var3.hasNext(); total += sst.getBloomFilterOffHeapSize()) {
               sst = (SSTableReader)var3.next();
            }

            return Long.valueOf(total);
         }
      });
      this.indexSummaryOffHeapMemoryUsed = this.createTableGauge("IndexSummaryOffHeapMemoryUsed", new Gauge<Long>() {
         public Long getValue() {
            long total = 0L;

            for(Iterator var3 = cfs.getSSTables(SSTableSet.LIVE).iterator(); var3.hasNext(); total += 0L) {
               SSTableReader sst = (SSTableReader)var3.next();
            }

            return Long.valueOf(total);
         }
      });
      this.compressionMetadataOffHeapMemoryUsed = this.createTableGauge("CompressionMetadataOffHeapMemoryUsed", new Gauge<Long>() {
         public Long getValue() {
            long total = 0L;

            SSTableReader sst;
            for(Iterator var3 = cfs.getSSTables(SSTableSet.LIVE).iterator(); var3.hasNext(); total += sst.getCompressionMetadataOffHeapSize()) {
               sst = (SSTableReader)var3.next();
            }

            return Long.valueOf(total);
         }
      });
      this.speculativeRetries = this.createTableCounter("SpeculativeRetries");
      this.speculativeFailedRetries = this.createTableCounter("SpeculativeFailedRetries");
      this.speculativeInsufficientReplicas = this.createTableCounter("SpeculativeInsufficientReplicas");
      this.speculativeSampleLatencyNanos = this.createTableGauge("SpeculativeSampleLatencyNanos", new Gauge<Long>() {
         public Long getValue() {
            return Long.valueOf(cfs.sampleLatencyNanos);
         }
      });
      this.keyCacheHitRate = (Gauge)CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName("KeyCacheHitRate"), this.aliasFactory.createMetricName("KeyCacheHitRate"), new RatioGauge() {
         public Ratio getRatio() {
            return Ratio.of(this.getNumerator(), this.getDenominator());
         }

         protected double getNumerator() {
            long hits = 0L;

            SSTableReader sstable;
            for(Iterator var3 = cfs.getSSTables(SSTableSet.LIVE).iterator(); var3.hasNext(); hits += sstable.getKeyCacheHit()) {
               sstable = (SSTableReader)var3.next();
            }

            return (double)hits;
         }

         protected double getDenominator() {
            long requests = 0L;

            SSTableReader sstable;
            for(Iterator var3 = cfs.getSSTables(SSTableSet.LIVE).iterator(); var3.hasNext(); requests += sstable.getKeyCacheRequest()) {
               sstable = (SSTableReader)var3.next();
            }

            return (double)Math.max(requests, 1L);
         }
      });
      this.tombstoneScannedHistogram = this.createTableHistogram("TombstoneScannedHistogram", cfs.keyspace.metric.tombstoneScannedHistogram);
      this.liveScannedHistogram = this.createTableHistogram("LiveScannedHistogram", cfs.keyspace.metric.liveScannedHistogram);
      this.colUpdateTimeDeltaHistogram = this.createTableHistogram("ColUpdateTimeDeltaHistogram", cfs.keyspace.metric.colUpdateTimeDeltaHistogram);
      this.coordinatorReadLatency = CassandraMetricsRegistry.Metrics.timer(this.factory.createMetricName("CoordinatorReadLatency"));
      this.coordinatorScanLatency = CassandraMetricsRegistry.Metrics.timer(this.factory.createMetricName("CoordinatorScanLatency"));
      this.waitingOnFreeMemtableSpace = CassandraMetricsRegistry.Metrics.histogram(this.factory.createMetricName("WaitingOnFreeMemtableSpace"), false);
      if(cfs.metadata().isView()) {
         this.viewLockAcquireTime = null;
         this.viewReadTime = null;
      } else {
         this.viewLockAcquireTime = this.createTableTimer("ViewLockAcquireTime", cfs.keyspace.metric.viewLockAcquireTime);
         this.viewReadTime = this.createTableTimer("ViewReadTime", cfs.keyspace.metric.viewReadTime);
      }

      this.trueSnapshotsSize = this.createTableGauge("SnapshotsSize", new Gauge<Long>() {
         public Long getValue() {
            return Long.valueOf(cfs.trueSnapshotsSize());
         }
      });
      this.rowCacheHitOutOfRange = this.createTableCounter("RowCacheHitOutOfRange");
      this.rowCacheHit = this.createTableCounter("RowCacheHit");
      this.rowCacheMiss = this.createTableCounter("RowCacheMiss");
      this.tombstoneFailures = this.createTableCounter("TombstoneFailures");
      this.tombstoneWarnings = this.createTableCounter("TombstoneWarnings");
      this.droppedMutations = this.createTableCounter("DroppedMutations");
      this.viewLockAcquisitionTimeouts = this.createTableCounter("ViewLockAcquisitionTimeouts");
      this.casPrepare = new LatencyMetrics(this.factory, this.aliasFactory, "CasPrepare", new LatencyMetrics[]{cfs.keyspace.metric.casPrepare});
      this.casPropose = new LatencyMetrics(this.factory, this.aliasFactory, "CasPropose", new LatencyMetrics[]{cfs.keyspace.metric.casPropose});
      this.casCommit = new LatencyMetrics(this.factory, this.aliasFactory, "CasCommit", new LatencyMetrics[]{cfs.keyspace.metric.casCommit});
      this.repairsStarted = this.createTableCounter("RepairJobsStarted");
      this.repairsCompleted = this.createTableCounter("RepairJobsCompleted");
      this.anticompactionTime = this.createTableTimer("AnticompactionTime", cfs.keyspace.metric.anticompactionTime);
      this.validationTime = this.createTableTimer("ValidationTime", cfs.keyspace.metric.validationTime);
      this.syncTime = this.createTableTimer("SyncTime", cfs.keyspace.metric.repairSyncTime);
      this.bytesValidated = this.createTableHistogram("BytesValidated", cfs.keyspace.metric.bytesValidated);
      this.partitionsValidated = this.createTableHistogram("PartitionsValidated", cfs.keyspace.metric.partitionsValidated);
      this.bytesAnticompacted = this.createTableCounter("BytesAnticompacted");
      this.bytesMutatedAnticompaction = this.createTableCounter("BytesMutatedAnticompaction");
      this.mutatedAnticompactionGauge = this.createTableGauge("MutatedAnticompactionGauge", () -> {
         double bytesMutated = (double)this.bytesMutatedAnticompaction.getCount();
         double bytesAnticomp = (double)this.bytesAnticompacted.getCount();
         return bytesAnticomp + bytesMutated > 0.0D?Double.valueOf(bytesMutated / (bytesAnticomp + bytesMutated)):Double.valueOf(0.0D);
      });
      this.nodeSyncMetrics = new NodeSyncMetrics(this.factory, "NodeSync");
      this.readRepairRequests = CassandraMetricsRegistry.Metrics.meter(this.factory.createMetricName("ReadRepairRequests"));
      this.shortReadProtectionRequests = CassandraMetricsRegistry.Metrics.meter(this.factory.createMetricName("ShortReadProtectionRequests"));
   }

   public void updateSSTableIterated(int count) {
      this.sstablesPerReadHistogram.update(count);
   }

   public void release() {
      Iterator var1 = all.entrySet().iterator();

      while(var1.hasNext()) {
         Entry<String, String> entry = (Entry)var1.next();
         CassandraMetricsRegistry.MetricName name = this.factory.createMetricName((String)entry.getKey());
         Metric metric = (Metric)CassandraMetricsRegistry.Metrics.getMetrics().get(name.getMetricName());
         if(metric != null) {
            CassandraMetricsRegistry.MetricName alias = this.aliasFactory.createMetricName((String)entry.getValue());
            ((Set)allTableMetrics.get(entry.getKey())).remove(metric);
            CassandraMetricsRegistry.Metrics.remove(name, alias);
         }
      }

      this.readLatency.release();
      this.writeLatency.release();
      this.rangeLatency.release();
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("EstimatedPartitionSizeHistogram"), this.aliasFactory.createMetricName("EstimatedRowSizeHistogram"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("EstimatedPartitionCount"), this.aliasFactory.createMetricName("EstimatedRowCount"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("EstimatedColumnCountHistogram"), this.aliasFactory.createMetricName("EstimatedColumnCountHistogram"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("KeyCacheHitRate"), this.aliasFactory.createMetricName("KeyCacheHitRate"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("CoordinatorReadLatency"), this.aliasFactory.createMetricName("CoordinatorReadLatency"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("CoordinatorScanLatency"), this.aliasFactory.createMetricName("CoordinatorScanLatency"));
      CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName("WaitingOnFreeMemtableSpace"), this.aliasFactory.createMetricName("WaitingOnFreeMemtableSpace"));
      this.nodeSyncMetrics.release();
   }

   protected <T extends Number> Gauge<T> createTableGauge(final String name, Gauge<T> gauge) {
      return this.createTableGauge(name, gauge, new Gauge<Long>() {
         public Long getValue() {
            long total = 0L;

            Metric cfGauge;
            for(Iterator var3 = ((Set)TableMetrics.allTableMetrics.get(name)).iterator(); var3.hasNext(); total += ((Number)((Gauge)cfGauge).getValue()).longValue()) {
               cfGauge = (Metric)var3.next();
            }

            return Long.valueOf(total);
         }
      });
   }

   protected <G, T> Gauge<T> createTableGauge(String name, Gauge<T> gauge, Gauge<G> globalGauge) {
      return this.createTableGauge(name, name, gauge, globalGauge);
   }

   protected <G, T> Gauge<T> createTableGauge(String name, String alias, Gauge<T> gauge, Gauge<G> globalGauge) {
      Gauge<T> cfGauge = (Gauge)CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName(name), this.aliasFactory.createMetricName(alias), gauge);
      if(this.register(name, alias, cfGauge)) {
         CassandraMetricsRegistry.Metrics.register(globalFactory.createMetricName(name), globalAliasFactory.createMetricName(alias), globalGauge);
      }

      return cfGauge;
   }

   protected Counter createTableCounter(String name) {
      return this.createTableCounter(name, name);
   }

   protected Counter createTableCounter(final String name, String alias) {
      Counter cfCounter = CassandraMetricsRegistry.Metrics.counter(this.factory.createMetricName(name), this.aliasFactory.createMetricName(alias), false);
      if(this.register(name, alias, cfCounter)) {
         CassandraMetricsRegistry.Metrics.register(globalFactory.createMetricName(name), globalAliasFactory.createMetricName(alias), new Gauge<Long>() {
            public Long getValue() {
               long total = 0L;

               Metric cfGauge;
               for(Iterator var3 = ((Set)TableMetrics.allTableMetrics.get(name)).iterator(); var3.hasNext(); total += ((Counter)cfGauge).getCount()) {
                  cfGauge = (Metric)var3.next();
               }

               return Long.valueOf(total);
            }
         });
      }

      return cfCounter;
   }

   private static Double computeCompressionRatio(Iterable<SSTableReader> sstables) {
      double compressedLengthSum = 0.0D;
      double dataLengthSum = 0.0D;
      Iterator var5 = sstables.iterator();

      while(var5.hasNext()) {
         SSTableReader sstable = (SSTableReader)var5.next();
         if(sstable.compression) {
            assert sstable.openReason != SSTableReader.OpenReason.EARLY;

            CompressionMetadata compressionMetadata = sstable.getCompressionMetadata();
            compressedLengthSum += (double)compressionMetadata.compressedFileLength;
            dataLengthSum += (double)compressionMetadata.dataLength;
         }
      }

      return Double.valueOf(dataLengthSum != 0.0D?compressedLengthSum / dataLengthSum:-1.0D);
   }

   protected Histogram createTableHistogram(String name, Histogram keyspaceHistogram) {
      return this.createTableHistogram(name, name, keyspaceHistogram);
   }

   protected Histogram createTableHistogram(String name, String alias, Histogram keyspaceHistogram) {
      boolean considerZeroes = keyspaceHistogram.considerZeroes();
      Histogram tableHistogram = CassandraMetricsRegistry.Metrics.histogram(this.factory.createMetricName(name), this.aliasFactory.createMetricName(alias), considerZeroes, false);
      this.register(name, alias, tableHistogram);
      Histogram globalHistogram = CassandraMetricsRegistry.Metrics.histogram(globalFactory.createMetricName(name), globalAliasFactory.createMetricName(alias), considerZeroes, true);
      keyspaceHistogram.compose(tableHistogram);
      globalHistogram.compose(tableHistogram);
      return tableHistogram;
   }

   protected Timer createTableTimer(String name, Timer keyspaceTimer) {
      return this.createTableTimer(name, name, keyspaceTimer);
   }

   protected Timer createTableTimer(String name, String alias, Timer keyspaceTimer) {
      Timer cfTimer = CassandraMetricsRegistry.Metrics.timer(this.factory.createMetricName(name), this.aliasFactory.createMetricName(alias), false);
      this.register(name, alias, cfTimer);
      Timer globalTimer = CassandraMetricsRegistry.Metrics.timer(globalFactory.createMetricName(name), globalAliasFactory.createMetricName(alias), true);
      keyspaceTimer.compose(cfTimer);
      globalTimer.compose(cfTimer);
      return cfTimer;
   }

   private boolean register(String name, String alias, Metric metric) {
      boolean ret = allTableMetrics.putIfAbsent(name, ConcurrentHashMap.newKeySet()) == null;
      ((Set)allTableMetrics.get(name)).add(metric);
      all.put(name, alias);
      return ret;
   }

   static {
      globalReadLatency = new LatencyMetrics(globalFactory, globalAliasFactory, "Read", true);
      globalWriteLatency = new LatencyMetrics(globalFactory, globalAliasFactory, "Write", true);
      globalRangeLatency = new LatencyMetrics(globalFactory, globalAliasFactory, "Range", true);
      globalPercentRepaired = (Gauge)CassandraMetricsRegistry.Metrics.register(globalFactory.createMetricName("PercentRepaired"), new Gauge<Double>() {
         public Double getValue() {
            Pair<Long, Long> result = TableMetrics.totalNonSystemTablesSize(SSTableReader::isRepaired);
            double repaired = (double)((Long)result.left).longValue();
            double total = (double)((Long)result.right).longValue();
            return Double.valueOf(total > 0.0D?repaired / total * 100.0D:100.0D);
         }
      });
      globalBytesRepaired = (Gauge)CassandraMetricsRegistry.Metrics.register(globalFactory.createMetricName("BytesRepaired"), new Gauge<Long>() {
         public Long getValue() {
            return (Long)TableMetrics.totalNonSystemTablesSize(SSTableReader::isRepaired).left;
         }
      });
      globalBytesUnrepaired = (Gauge)CassandraMetricsRegistry.Metrics.register(globalFactory.createMetricName("BytesUnrepaired"), new Gauge<Long>() {
         public Long getValue() {
            return (Long)TableMetrics.totalNonSystemTablesSize((s) -> {
               return !s.isRepaired() && !s.isPendingRepair();
            }).left;
         }
      });
      globalBytesPendingRepair = (Gauge)CassandraMetricsRegistry.Metrics.register(globalFactory.createMetricName("BytesPendingRepair"), new Gauge<Long>() {
         public Long getValue() {
            return (Long)TableMetrics.totalNonSystemTablesSize(SSTableReader::isPendingRepair).left;
         }
      });
      allTableMetrics = Maps.newConcurrentMap();
      all = Maps.newConcurrentMap();
   }

   public static enum Sampler {
      READS,
      WRITES;

      private Sampler() {
      }
   }

   private static class AllTableMetricNameFactory extends AbstractMetricNameFactory {
      private AllTableMetricNameFactory(String type) {
         super(TableMetrics.class.getPackage().getName(), type);
      }
   }

   private static class TableMetricNameFactory extends AbstractMetricNameFactory {
      private TableMetricNameFactory(ColumnFamilyStore cfs, String type) {
         super(TableMetrics.class.getPackage().getName(), cfs.isIndex()?"Index" + type:type, cfs.keyspace.getName(), (String)null, cfs.name);
      }
   }

   private interface GetHistogram {
      EstimatedHistogram getHistogram(SSTableReader var1);
   }
}
