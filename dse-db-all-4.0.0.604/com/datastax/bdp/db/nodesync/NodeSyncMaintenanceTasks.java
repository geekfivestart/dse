package com.datastax.bdp.db.nodesync;

import com.google.common.base.Joiner;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.metrics.NodeSyncMetrics;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.units.RateValue;
import org.apache.cassandra.utils.units.SizeUnit;
import org.apache.cassandra.utils.units.SizeValue;
import org.apache.cassandra.utils.units.TimeValue;
import org.apache.cassandra.utils.units.Units;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class NodeSyncMaintenanceTasks {
   private static final long LOG_REPORTING_DELAY_SEC;
   private static final long RATE_CHECKING_DELAY_SEC;
   private static final long SIZE_CHECKING_DELAY_SEC;
   private static final Logger logger;
   private final ScheduledExecutorService scheduledExecutor = new DebuggableScheduledThreadPoolExecutor("NodeSyncMaintenanceTasks");
   private final NodeSyncService.Instance instance;
   private final NodeSyncMaintenanceTasks.LogReporter logReporter;
   private final NodeSyncMaintenanceTasks.RateChecker rateChecker;
   private final NodeSyncMaintenanceTasks.SizeChecker sizeChecker;

   NodeSyncMaintenanceTasks(NodeSyncService.Instance instance) {
      this.instance = instance;
      this.logReporter = new NodeSyncMaintenanceTasks.LogReporter();
      this.rateChecker = new NodeSyncMaintenanceTasks.RateChecker();
      this.sizeChecker = new NodeSyncMaintenanceTasks.SizeChecker();
   }

   void start() {
      this.logReporter.start();
      this.rateChecker.start();
      this.sizeChecker.start();
   }

   void stop() {
      this.logReporter.stop();
      this.rateChecker.stop();
      this.sizeChecker.stop();
      this.scheduledExecutor.shutdown();
   }

   void onRateUpdate() {
      this.rateChecker.onRateUpdate();
   }

   static {
      LOG_REPORTING_DELAY_SEC = Long.getLong("dse.nodesync.log_reporter_interval_sec", TimeUnit.MINUTES.toSeconds(10L)).longValue();
      RATE_CHECKING_DELAY_SEC = Long.getLong("dse.nodesync.rate_checker_interval_sec", TimeUnit.MINUTES.toSeconds(30L)).longValue();
      SIZE_CHECKING_DELAY_SEC = Long.getLong("dse.nodesync.size_checker_interval_sec", TimeUnit.HOURS.toSeconds(2L)).longValue();
      logger = LoggerFactory.getLogger(NodeSyncMaintenanceTasks.class);
   }

   private class SizeChecker extends NodeSyncMaintenanceTasks.ScheduledTask {
      private SizeChecker() {
         super(null);
      }

      long delayInSec() {
         return NodeSyncMaintenanceTasks.SIZE_CHECKING_DELAY_SEC;
      }

      public void run() {
         NodeSyncHelpers.nodeSyncEnabledStores().forEach(this::checkTable);
      }

      private void checkTable(ColumnFamilyStore store) {
         TableMetadata table = store.metadata();
         TableState state = NodeSyncMaintenanceTasks.this.instance.state.get(table);
         if(state != null) {
            long size = NodeSyncHelpers.estimatedSizeOf(store);
            int currentDepth = state.depth();
            Collection<Range<Token>> localRanges = state.localRanges();
            long segSizeIncr = NodeSyncHelpers.segmentSizeTarget() + NodeSyncHelpers.segmentSizeTarget() / 4L;
            int newDepth = Segments.depth(size, localRanges.size(), segSizeIncr);
            if(newDepth > currentDepth) {
               this.updateDepth(state, size, currentDepth, newDepth, "Increasing");
            } else if(currentDepth != 0) {
               long segSizeDecr = NodeSyncHelpers.segmentSizeTarget() / 2L;
               newDepth = Segments.depth(size, localRanges.size(), segSizeDecr);
               if(newDepth < currentDepth) {
                  this.updateDepth(state, size, currentDepth, newDepth, "Decreasing");
               }

            }
         }
      }

      private void updateDepth(TableState state, long tableSize, int currentDepth, int newDepth, String action) {
         TableMetadata table = state.table();
         Collection<Range<Token>> localRanges = state.localRanges();
         int currSegCount = Segments.estimateSegments(localRanges, currentDepth);
         int newSegCount = Segments.estimateSegments(localRanges, newDepth);
         SizeValue segmentTarget = SizeValue.of(NodeSyncHelpers.segmentSizeTarget(), SizeUnit.BYTES);
         SizeValue currentPerSegment = SizeValue.of(tableSize / (long)currSegCount, SizeUnit.BYTES);
         SizeValue newPerSegment = SizeValue.of(tableSize / (long)newSegCount, SizeUnit.BYTES);
         NodeSyncMaintenanceTasks.logger.info("{} number of segments for table {} (from {} to {}) to account for recent data size change. Table size is {} and current depth is {}, so ~{} per segment for a target of maximum {} per segment; {} depth to {}, so ~{} per segment after update.", new Object[]{action, table, Integer.valueOf(currSegCount), Integer.valueOf(newSegCount), Units.toString(tableSize, SizeUnit.BYTES), Integer.valueOf(currentDepth), currentPerSegment, segmentTarget, action.toLowerCase(), Integer.valueOf(newDepth), newPerSegment});
         state.update(newDepth);
      }
   }

   private class RateChecker extends NodeSyncMaintenanceTasks.ScheduledTask {
      private long lastInsufficientRateWarn;
      private long lastLowRateInfo;

      private RateChecker() {
         super(null);
         this.lastInsufficientRateWarn = -1L;
         this.lastLowRateInfo = -1L;
      }

      long delayInSec() {
         return NodeSyncMaintenanceTasks.RATE_CHECKING_DELAY_SEC;
      }

      void start() {
         this.checkRate();
         super.start();
      }

      public void run() {
         this.checkRate();
      }

      private boolean checkRate() {
         long now = System.currentTimeMillis();
         if(this.lastInsufficientRateWarn >= 0L && now - this.lastInsufficientRateWarn < NodeSyncService.MIN_WARN_INTERVAL_MS) {
            return false;
         } else {
            RateValue rate = NodeSyncMaintenanceTasks.this.instance.service().config().getRate();
            RateSimulator.Info info = RateSimulator.Info.compute(false);
            RateValue minimumTheoretical = (new RateSimulator(info, RateSimulator.Parameters.THEORETICAL_MINIMUM)).computeRate();
            RateValue minimumRecommended = (new RateSimulator(info, RateSimulator.Parameters.MINIMUM_RECOMMENDED)).computeRate();
            RateValue recommended;
            if(rate.compareTo(minimumTheoretical) < 0) {
               recommended = (new RateSimulator(info, RateSimulator.Parameters.RECOMMENDED)).computeRate();
               NodeSyncMaintenanceTasks.logger.warn("The configured NodeSync rate on this node ({}) is too low to possibly validate all NodeSync-enabled tables within their respective deadline ('deadline_target_sec' property). This can be fixed by increasing the rate and/or increasing table deadlines. With the current deadlines and current table size, the theoretical minimum rate would be {}, but we would recommend a _minimum_ of {} and ideally {} to account for node failures, temporary slow nodes and future data growth. Please check 'nodetool nodesyncservice ratesimulator' for more details on how those values are computed.", new Object[]{rate, minimumTheoretical, minimumRecommended, recommended});
               this.lastInsufficientRateWarn = now;
               return true;
            } else if(this.lastLowRateInfo >= 0L && now - this.lastLowRateInfo < NodeSyncService.MIN_WARN_INTERVAL_MS) {
               return false;
            } else if(rate.compareTo(minimumRecommended) >= 0) {
               return false;
            } else {
               recommended = (new RateSimulator(info, RateSimulator.Parameters.RECOMMENDED)).computeRate();
               NodeSyncMaintenanceTasks.logger.info("The configured NodeSync rate on this node ({}) is barely above the theoretical minimum ({})necessary to validate all NodeSync-enabled tables within their respective deadline ('deadline_target_sec' property). This makes it likely those deadline may not be met in the face of relatively normal events like temporary slow or failed nodes, and don't account forfuture data growth. We would recommend a _minimum_ of {} and ideally {}. Alternatively, you can also relax the deadlines on tables (by updating the 'deadline_target_sec' property). Please check 'nodetool nodesyncservice ratesimulator' for more details on how those ratesvalues are computed.", new Object[]{rate, minimumTheoretical, minimumRecommended, recommended});
               this.lastLowRateInfo = now;
               return true;
            }
         }
      }

      private synchronized void onRateUpdate() {
         this.stop();
         boolean hadLoggedRecently = this.lastInsufficientRateWarn >= 0L || this.lastLowRateInfo >= 0L;
         this.lastInsufficientRateWarn = -1L;
         this.lastLowRateInfo = -1L;
         boolean logged = this.checkRate();
         if(!logged) {
            NodeSyncMaintenanceTasks.logger.info("Updated configured rate to {}{}.", NodeSyncMaintenanceTasks.this.instance.service().config().getRate(), hadLoggedRecently?" (the new rate is now above the recommend minimum)":"");
         }

         this.start();
      }
   }

   private class LogReporter extends NodeSyncMaintenanceTasks.ScheduledTask {
      private final TimeValue LOG_INTERVAL;
      private int lastValidatedTables;
      private long lastScheduledValidations;
      private long lastValidatedBytes;
      private long lastRepairedBytes;
      private long lastProcessedPages;
      private long lastPartialPages;
      private long lastUncompletedPages;
      private long lastFailedPages;

      private LogReporter() {
         super(null);
         this.LOG_INTERVAL = TimeValue.of(NodeSyncMaintenanceTasks.LOG_REPORTING_DELAY_SEC, TimeUnit.SECONDS);
         this.lastValidatedBytes = 0L;
         this.lastRepairedBytes = 0L;
         this.lastProcessedPages = 0L;
         this.lastPartialPages = 0L;
         this.lastUncompletedPages = 0L;
         this.lastFailedPages = 0L;
         this.lastValidatedTables = this.scheduler().continuouslyValidatedTables();
         this.lastScheduledValidations = this.scheduler().scheduledValidations();
      }

      private ValidationScheduler scheduler() {
         return NodeSyncMaintenanceTasks.this.instance.scheduler;
      }

      private NodeSyncMetrics metrics() {
         return NodeSyncMaintenanceTasks.this.instance.service().metrics();
      }

      long delayInSec() {
         return NodeSyncMaintenanceTasks.LOG_REPORTING_DELAY_SEC;
      }

      public void run() {
         int currentValidatedTables = this.scheduler().continuouslyValidatedTables();
         long currentScheduledValidations = this.scheduler().scheduledValidations();
         long currentValidatedBytes = this.metrics().dataValidated.getCount();
         long currentRepairedBytes = this.metrics().dataRepaired.getCount();
         long currentProcessedPages = this.metrics().processedPages.getCount();
         long currentPartialPages = this.metrics().partialInSyncPages.getCount() + this.metrics().partialRepairedPages.getCount();
         long currentUncompletedPages = this.metrics().uncompletedPages.getCount();
         long currentFailedPages = this.metrics().failedPages.getCount();
         if(currentScheduledValidations != this.lastScheduledValidations || this.lastValidatedTables != 0 || currentValidatedTables != 0) {
            long validatedDiff = currentValidatedBytes - this.lastValidatedBytes;
            SizeValue validatedBytes = SizeValue.of(validatedDiff, SizeUnit.BYTES);
            long diffProcessedPages = currentProcessedPages - this.lastProcessedPages;
            long diffPartialPages = currentPartialPages - this.lastPartialPages;
            long diffUncompletedPages = currentUncompletedPages - this.lastUncompletedPages;
            long diffFailedPages = currentFailedPages - this.lastFailedPages;
            List<String> details = new ArrayList();
            if(diffPartialPages > 0L) {
               details.add(String.format("%d%% partial", new Object[]{Integer.valueOf(this.percent(diffPartialPages, diffProcessedPages))}));
            }

            if(diffUncompletedPages > 0L) {
               details.add(String.format("%d%% uncompleted", new Object[]{Integer.valueOf(this.percent(diffUncompletedPages, diffProcessedPages))}));
            }

            if(diffFailedPages > 0L) {
               details.add(String.format("%d%% failed", new Object[]{Integer.valueOf(this.percent(diffFailedPages, diffProcessedPages))}));
            }

            String detailStr = details.isEmpty()?"":'(' + Joiner.on(',').join(details) + ')';
            NodeSyncMaintenanceTasks.logger.info("In last {}: validated {} ({}), {}% was inconsistent{}.", new Object[]{this.LOG_INTERVAL, validatedBytes, RateValue.compute(validatedBytes, this.LOG_INTERVAL), Integer.valueOf(this.percent(currentRepairedBytes - this.lastRepairedBytes, validatedDiff)), detailStr});
            this.lastValidatedTables = currentValidatedTables;
            this.lastScheduledValidations = currentScheduledValidations;
            this.lastValidatedBytes = currentValidatedBytes;
            this.lastRepairedBytes = currentRepairedBytes;
            this.lastProcessedPages = currentProcessedPages;
            this.lastPartialPages = currentPartialPages;
            this.lastUncompletedPages = currentUncompletedPages;
            this.lastFailedPages = currentFailedPages;
         }
      }

      private int percent(long value, long total) {
         return value == 0L?0:Math.min((int)(value * 100L / total), 100);
      }
   }

   private abstract class ScheduledTask implements Runnable {
      private volatile ScheduledFuture<?> future;

      private ScheduledTask() {
      }

      abstract long delayInSec();

      void start() {
         assert this.future == null : "Already started";

         long delayInSec = this.delayInSec();
         this.future = NodeSyncMaintenanceTasks.this.scheduledExecutor.scheduleAtFixedRate(this, delayInSec, delayInSec, TimeUnit.SECONDS);
      }

      void stop() {
         if(this.future != null) {
            this.future.cancel(true);
         }

         this.future = null;
      }
   }
}
