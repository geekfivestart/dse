package org.apache.cassandra.metrics;

import com.codahale.metrics.Gauge;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.Iterator;
import java.util.Set;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;

public class KeyspaceMetrics {
   public final Gauge<Long> memtableLiveDataSize;
   public final Gauge<Long> memtableOnHeapDataSize;
   public final Gauge<Long> memtableOffHeapDataSize;
   public final Gauge<Long> allMemtablesLiveDataSize;
   public final Gauge<Long> allMemtablesOnHeapDataSize;
   public final Gauge<Long> allMemtablesOffHeapDataSize;
   public final Gauge<Long> memtableColumnsCount;
   public final Gauge<Long> memtableSwitchCount;
   public final Gauge<Long> pendingFlushes;
   public final Gauge<Long> pendingCompactions;
   public final Gauge<Long> liveDiskSpaceUsed;
   public final Gauge<Long> totalDiskSpaceUsed;
   public final Gauge<Long> bloomFilterDiskSpaceUsed;
   public final Gauge<Long> bloomFilterOffHeapMemoryUsed;
   public final Gauge<Long> indexSummaryOffHeapMemoryUsed;
   public final Gauge<Long> compressionMetadataOffHeapMemoryUsed;
   public final LatencyMetrics readLatency;
   public final LatencyMetrics rangeLatency;
   public final LatencyMetrics writeLatency;
   public final Histogram sstablesPerReadHistogram;
   public final Histogram tombstoneScannedHistogram;
   public final Histogram liveScannedHistogram;
   public final Histogram colUpdateTimeDeltaHistogram;
   public final Timer viewLockAcquireTime;
   public final Timer viewReadTime;
   public final LatencyMetrics casPrepare;
   public final LatencyMetrics casPropose;
   public final LatencyMetrics casCommit;
   public final Counter writeFailedIdealCL;
   public final LatencyMetrics idealCLWriteLatency;
   public final Counter speculativeRetries;
   public final Counter speculativeFailedRetries;
   public final Counter speculativeInsufficientReplicas;
   public final Counter repairsStarted;
   public final Counter repairsCompleted;
   public final Timer repairTime;
   public final Timer repairPrepareTime;
   public final Timer anticompactionTime;
   public final Timer validationTime;
   public final Timer repairSyncTime;
   public final Histogram bytesValidated;
   public final Histogram partitionsValidated;
   public final MetricNameFactory factory;
   private Keyspace keyspace;
   private Set<String> allMetrics = Sets.newHashSet();

   public KeyspaceMetrics(Keyspace ks) {
      this.factory = new KeyspaceMetrics.KeyspaceMetricNameFactory(ks, null);
      this.keyspace = ks;
      this.memtableColumnsCount = this.createKeyspaceGauge("MemtableColumnsCount", new KeyspaceMetrics.MetricValue() {
         public Long getValue(TableMetrics metric) {
            return (Long)metric.memtableColumnsCount.getValue();
         }
      });
      this.memtableLiveDataSize = this.createKeyspaceGauge("MemtableLiveDataSize", new KeyspaceMetrics.MetricValue() {
         public Long getValue(TableMetrics metric) {
            return (Long)metric.memtableLiveDataSize.getValue();
         }
      });
      this.memtableOnHeapDataSize = this.createKeyspaceGauge("MemtableOnHeapDataSize", new KeyspaceMetrics.MetricValue() {
         public Long getValue(TableMetrics metric) {
            return (Long)metric.memtableOnHeapSize.getValue();
         }
      });
      this.memtableOffHeapDataSize = this.createKeyspaceGauge("MemtableOffHeapDataSize", new KeyspaceMetrics.MetricValue() {
         public Long getValue(TableMetrics metric) {
            return (Long)metric.memtableOffHeapSize.getValue();
         }
      });
      this.allMemtablesLiveDataSize = this.createKeyspaceGauge("AllMemtablesLiveDataSize", new KeyspaceMetrics.MetricValue() {
         public Long getValue(TableMetrics metric) {
            return (Long)metric.allMemtablesLiveDataSize.getValue();
         }
      });
      this.allMemtablesOnHeapDataSize = this.createKeyspaceGauge("AllMemtablesOnHeapDataSize", new KeyspaceMetrics.MetricValue() {
         public Long getValue(TableMetrics metric) {
            return (Long)metric.allMemtablesOnHeapSize.getValue();
         }
      });
      this.allMemtablesOffHeapDataSize = this.createKeyspaceGauge("AllMemtablesOffHeapDataSize", new KeyspaceMetrics.MetricValue() {
         public Long getValue(TableMetrics metric) {
            return (Long)metric.allMemtablesOffHeapSize.getValue();
         }
      });
      this.memtableSwitchCount = this.createKeyspaceGauge("MemtableSwitchCount", new KeyspaceMetrics.MetricValue() {
         public Long getValue(TableMetrics metric) {
            return Long.valueOf(metric.memtableSwitchCount.getCount());
         }
      });
      this.pendingCompactions = this.createKeyspaceGauge("PendingCompactions", new KeyspaceMetrics.MetricValue() {
         public Long getValue(TableMetrics metric) {
            return Long.valueOf((long)((Integer)metric.pendingCompactions.getValue()).intValue());
         }
      });
      this.pendingFlushes = this.createKeyspaceGauge("PendingFlushes", new KeyspaceMetrics.MetricValue() {
         public Long getValue(TableMetrics metric) {
            return Long.valueOf(metric.pendingFlushes.getCount());
         }
      });
      this.liveDiskSpaceUsed = this.createKeyspaceGauge("LiveDiskSpaceUsed", new KeyspaceMetrics.MetricValue() {
         public Long getValue(TableMetrics metric) {
            return Long.valueOf(metric.liveDiskSpaceUsed.getCount());
         }
      });
      this.totalDiskSpaceUsed = this.createKeyspaceGauge("TotalDiskSpaceUsed", new KeyspaceMetrics.MetricValue() {
         public Long getValue(TableMetrics metric) {
            return Long.valueOf(metric.totalDiskSpaceUsed.getCount());
         }
      });
      this.bloomFilterDiskSpaceUsed = this.createKeyspaceGauge("BloomFilterDiskSpaceUsed", new KeyspaceMetrics.MetricValue() {
         public Long getValue(TableMetrics metric) {
            return (Long)metric.bloomFilterDiskSpaceUsed.getValue();
         }
      });
      this.bloomFilterOffHeapMemoryUsed = this.createKeyspaceGauge("BloomFilterOffHeapMemoryUsed", new KeyspaceMetrics.MetricValue() {
         public Long getValue(TableMetrics metric) {
            return (Long)metric.bloomFilterOffHeapMemoryUsed.getValue();
         }
      });
      this.indexSummaryOffHeapMemoryUsed = this.createKeyspaceGauge("IndexSummaryOffHeapMemoryUsed", new KeyspaceMetrics.MetricValue() {
         public Long getValue(TableMetrics metric) {
            return (Long)metric.indexSummaryOffHeapMemoryUsed.getValue();
         }
      });
      this.compressionMetadataOffHeapMemoryUsed = this.createKeyspaceGauge("CompressionMetadataOffHeapMemoryUsed", new KeyspaceMetrics.MetricValue() {
         public Long getValue(TableMetrics metric) {
            return (Long)metric.compressionMetadataOffHeapMemoryUsed.getValue();
         }
      });
      this.readLatency = new LatencyMetrics(this.factory, "Read", true);
      this.writeLatency = new LatencyMetrics(this.factory, "Write", true);
      this.rangeLatency = new LatencyMetrics(this.factory, "Range", true);
      this.sstablesPerReadHistogram = CassandraMetricsRegistry.Metrics.histogram(this.factory.createMetricName("SSTablesPerReadHistogram"), true, true);
      this.tombstoneScannedHistogram = CassandraMetricsRegistry.Metrics.histogram(this.factory.createMetricName("TombstoneScannedHistogram"), false, true);
      this.liveScannedHistogram = CassandraMetricsRegistry.Metrics.histogram(this.factory.createMetricName("LiveScannedHistogram"), false, true);
      this.colUpdateTimeDeltaHistogram = CassandraMetricsRegistry.Metrics.histogram(this.factory.createMetricName("ColUpdateTimeDeltaHistogram"), false, true);
      this.viewLockAcquireTime = CassandraMetricsRegistry.Metrics.timer(this.factory.createMetricName("ViewLockAcquireTime"), true);
      this.viewReadTime = CassandraMetricsRegistry.Metrics.timer(this.factory.createMetricName("ViewReadTime"), true);
      this.allMetrics.addAll(Lists.newArrayList(new String[]{"SSTablesPerReadHistogram", "TombstoneScannedHistogram", "LiveScannedHistogram"}));
      this.casPrepare = new LatencyMetrics(this.factory, "CasPrepare", true);
      this.casPropose = new LatencyMetrics(this.factory, "CasPropose", true);
      this.casCommit = new LatencyMetrics(this.factory, "CasCommit", true);
      this.writeFailedIdealCL = CassandraMetricsRegistry.Metrics.counter(this.factory.createMetricName("WriteFailedIdealCL"));
      this.idealCLWriteLatency = new LatencyMetrics(this.factory, "IdealCLWrite", false);
      this.speculativeRetries = this.createKeyspaceCounter("SpeculativeRetries", new KeyspaceMetrics.MetricValue() {
         public Long getValue(TableMetrics metric) {
            return Long.valueOf(metric.speculativeRetries.getCount());
         }
      });
      this.speculativeFailedRetries = this.createKeyspaceCounter("SpeculativeFailedRetries", new KeyspaceMetrics.MetricValue() {
         public Long getValue(TableMetrics metric) {
            return Long.valueOf(metric.speculativeFailedRetries.getCount());
         }
      });
      this.speculativeInsufficientReplicas = this.createKeyspaceCounter("SpeculativeInsufficientReplicas", new KeyspaceMetrics.MetricValue() {
         public Long getValue(TableMetrics metric) {
            return Long.valueOf(metric.speculativeInsufficientReplicas.getCount());
         }
      });
      this.repairsStarted = this.createKeyspaceCounter("RepairJobsStarted", new KeyspaceMetrics.MetricValue() {
         public Long getValue(TableMetrics metric) {
            return Long.valueOf(metric.repairsStarted.getCount());
         }
      });
      this.repairsCompleted = this.createKeyspaceCounter("RepairJobsCompleted", new KeyspaceMetrics.MetricValue() {
         public Long getValue(TableMetrics metric) {
            return Long.valueOf(metric.repairsCompleted.getCount());
         }
      });
      this.repairTime = CassandraMetricsRegistry.Metrics.timer(this.factory.createMetricName("RepairTime"));
      this.repairPrepareTime = CassandraMetricsRegistry.Metrics.timer(this.factory.createMetricName("RepairPrepareTime"));
      this.anticompactionTime = CassandraMetricsRegistry.Metrics.timer(this.factory.createMetricName("AntiCompactionTime"), true);
      this.validationTime = CassandraMetricsRegistry.Metrics.timer(this.factory.createMetricName("ValidationTime"), true);
      this.repairSyncTime = CassandraMetricsRegistry.Metrics.timer(this.factory.createMetricName("RepairSyncTime"), true);
      this.partitionsValidated = CassandraMetricsRegistry.Metrics.histogram(this.factory.createMetricName("PartitionsValidated"), false, true);
      this.bytesValidated = CassandraMetricsRegistry.Metrics.histogram(this.factory.createMetricName("BytesValidated"), false, true);
   }

   public void release() {
      Iterator var1 = this.allMetrics.iterator();

      while(var1.hasNext()) {
         String name = (String)var1.next();
         CassandraMetricsRegistry.Metrics.remove(this.factory.createMetricName(name));
      }

      this.readLatency.release();
      this.writeLatency.release();
      this.rangeLatency.release();
      this.idealCLWriteLatency.release();
   }

   private Gauge<Long> createKeyspaceGauge(String name, final KeyspaceMetrics.MetricValue extractor) {
      this.allMetrics.add(name);
      return (Gauge)CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName(name), new Gauge<Long>() {
         public Long getValue() {
            long sum = 0L;

            ColumnFamilyStore cf;
            for(Iterator var3 = KeyspaceMetrics.this.keyspace.getColumnFamilyStores().iterator(); var3.hasNext(); sum += extractor.getValue(cf.metric).longValue()) {
               cf = (ColumnFamilyStore)var3.next();
            }

            return Long.valueOf(sum);
         }
      });
   }

   private Counter createKeyspaceCounter(String name, final KeyspaceMetrics.MetricValue extractor) {
      this.allMetrics.add(name);
      return (Counter)CassandraMetricsRegistry.Metrics.register(this.factory.createMetricName(name), new SingleCounter() {
         public long getCount() {
            long sum = 0L;

            ColumnFamilyStore cf;
            for(Iterator var3 = KeyspaceMetrics.this.keyspace.getColumnFamilyStores().iterator(); var3.hasNext(); sum += extractor.getValue(cf.metric).longValue()) {
               cf = (ColumnFamilyStore)var3.next();
            }

            return sum;
         }
      });
   }

   private static class KeyspaceMetricNameFactory extends AbstractMetricNameFactory {
      private KeyspaceMetricNameFactory(Keyspace ks) {
         super(TableMetrics.class.getPackage().getName(), "Keyspace", ks.getName(), (String)null, (String)null);
      }
   }

   private interface MetricValue {
      Long getValue(TableMetrics var1);
   }
}
