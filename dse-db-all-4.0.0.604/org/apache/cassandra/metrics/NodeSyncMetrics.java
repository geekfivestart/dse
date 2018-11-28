package org.apache.cassandra.metrics;

import com.datastax.bdp.db.nodesync.ValidationOutcome;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class NodeSyncMetrics {
   public final Meter dataValidated;
   public final Meter dataRepaired;
   private final Meter objectsValidated;
   private final Meter objectsRepaired;
   private final Meter repairDataSent;
   private final Meter repairObjectsSent;
   public final Counter processedPages;
   public final Counter fullInSyncPages;
   public final Counter fullRepairedPages;
   public final Counter partialInSyncPages;
   public final Counter partialRepairedPages;
   public final Counter uncompletedPages;
   public final Counter failedPages;
   private final MetricNameFactory factory;
   private final String namePrefix;
   private final Set<CassandraMetricsRegistry.MetricName> metricsNames = new HashSet();

   public NodeSyncMetrics(MetricNameFactory factory, String namePrefix) {
      this.factory = factory;
      this.namePrefix = namePrefix;
      this.dataValidated = CassandraMetricsRegistry.Metrics.meter(this.name("DataValidated"));
      this.dataRepaired = CassandraMetricsRegistry.Metrics.meter(this.name("DataRepaired"));
      this.repairDataSent = CassandraMetricsRegistry.Metrics.meter(this.name("RepairDataSent"));
      this.objectsValidated = CassandraMetricsRegistry.Metrics.meter(this.name("ObjectsValidated"));
      this.objectsRepaired = CassandraMetricsRegistry.Metrics.meter(this.name("ObjectsRepaired"));
      this.repairObjectsSent = CassandraMetricsRegistry.Metrics.meter(this.name("RepairObjectsSent"));
      this.processedPages = CassandraMetricsRegistry.Metrics.counter(this.name("ProcessedPages"));
      this.fullInSyncPages = CassandraMetricsRegistry.Metrics.counter(this.name("FullInSyncPages"));
      this.fullRepairedPages = CassandraMetricsRegistry.Metrics.counter(this.name("FullRepairedPages"));
      this.partialInSyncPages = CassandraMetricsRegistry.Metrics.counter(this.name("PartialInSyncPages"));
      this.partialRepairedPages = CassandraMetricsRegistry.Metrics.counter(this.name("PartialRepairedPages"));
      this.uncompletedPages = CassandraMetricsRegistry.Metrics.counter(this.name("UncompletedPages"));
      this.failedPages = CassandraMetricsRegistry.Metrics.counter(this.name("FailedPages"));
   }

   public void incrementObjects(long validated, long repaired) {
      this.objectsValidated.mark(validated);
      this.objectsRepaired.mark(repaired);
   }

   public void incrementDataSizes(long validated, long repaired) {
      this.dataValidated.mark(validated);
      this.dataRepaired.mark(repaired);
   }

   public void incrementRepairSent(long dataSent, long objectsSent) {
      this.repairDataSent.mark(dataSent);
      this.repairObjectsSent.mark(objectsSent);
   }

   public void addPageOutcomes(ValidationOutcome outcome, long pageCount) {
      this.processedPages.inc(pageCount);
      switch (outcome) {
         case FULL_IN_SYNC: {
            this.fullInSyncPages.inc(pageCount);
            break;
         }
         case FULL_REPAIRED: {
            this.fullRepairedPages.inc(pageCount);
            break;
         }
         case PARTIAL_IN_SYNC: {
            this.partialInSyncPages.inc(pageCount);
            break;
         }
         case PARTIAL_REPAIRED: {
            this.partialRepairedPages.inc(pageCount);
            break;
         }
         case UNCOMPLETED: {
            this.uncompletedPages.inc(pageCount);
            break;
         }
         case FAILED: {
            this.failedPages.inc(pageCount);
         }
      }
   }

   private CassandraMetricsRegistry.MetricName name(String name) {
      CassandraMetricsRegistry.MetricName metricName = this.factory.createMetricName(this.namePrefix + name);
      this.metricsNames.add(metricName);
      return metricName;
   }

   public void release() {
      Iterator var1 = this.metricsNames.iterator();

      while(var1.hasNext()) {
         CassandraMetricsRegistry.MetricName name = (CassandraMetricsRegistry.MetricName)var1.next();
         CassandraMetricsRegistry.Metrics.remove(name);
      }

   }
}
