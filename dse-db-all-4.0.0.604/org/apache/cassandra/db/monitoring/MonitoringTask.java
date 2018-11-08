package org.apache.cassandra.db.monitoring;

import com.google.common.annotations.VisibleForTesting;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.utils.NoSpamLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MonitoringTask {
   private static final String LINE_SEPARATOR = System.getProperty("line.separator");
   private static final Logger logger = LoggerFactory.getLogger(MonitoringTask.class);
   private static final NoSpamLogger noSpamLogger;
   private static final int REPORT_INTERVAL_MS;
   private static final int MAX_OPERATIONS;
   @VisibleForTesting
   static MonitoringTask instance;
   private final ScheduledFuture<?> reportingTask;
   private final MonitoringTask.OperationsQueue failedOperationsQueue;
   private final MonitoringTask.OperationsQueue slowOperationsQueue;
   private long lastLogTime;

   @VisibleForTesting
   static MonitoringTask make(int reportIntervalMillis, int maxTimedoutOperations) {
      if(instance != null) {
         instance.cancel();
         instance = null;
      }

      return new MonitoringTask(reportIntervalMillis, maxTimedoutOperations);
   }

   private MonitoringTask(int reportIntervalMillis, int maxOperations) {
      this.failedOperationsQueue = new MonitoringTask.OperationsQueue(maxOperations);
      this.slowOperationsQueue = new MonitoringTask.OperationsQueue(maxOperations);
      this.lastLogTime = ApproximateTime.currentTimeMillis();
      logger.info("Scheduling monitoring task with report interval of {} ms, max operations {}", Integer.valueOf(reportIntervalMillis), Integer.valueOf(maxOperations));
      this.reportingTask = ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(() -> {
         this.logOperations(ApproximateTime.currentTimeMillis());
      }, (long)reportIntervalMillis, (long)reportIntervalMillis, TimeUnit.MILLISECONDS);
   }

   public void cancel() {
      this.reportingTask.cancel(false);
   }

   static void addFailedOperation(Monitor monitor, long now) {
      instance.failedOperationsQueue.offer(new MonitoringTask.FailedOperation(monitor, now));
   }

   static void addSlowOperation(Monitor monitor, long now) {
      instance.slowOperationsQueue.offer(new MonitoringTask.SlowOperation(monitor, now));
   }

   @VisibleForTesting
   List<String> getFailedOperations() {
      return this.getLogMessages(this.failedOperationsQueue.popOperations());
   }

   @VisibleForTesting
   List<String> getSlowOperations() {
      return this.getLogMessages(this.slowOperationsQueue.popOperations());
   }

   private List<String> getLogMessages(MonitoringTask.AggregatedOperations operations) {
      String ret = operations.getLogMessage();
      return ret.isEmpty()?Collections.emptyList():Arrays.asList(ret.split("\n"));
   }

   @VisibleForTesting
   private void logOperations(long now) {
      this.logSlowOperations(now);
      this.logFailedOperations(now);
      this.lastLogTime = now;
   }

   @VisibleForTesting
   boolean logFailedOperations(long now) {
      MonitoringTask.AggregatedOperations failedOperations = this.failedOperationsQueue.popOperations();
      if(!failedOperations.isEmpty()) {
         long elapsed = now - this.lastLogTime;
         noSpamLogger.warn("Some operations timed out, details available at debug level (debug.log)", new Object[0]);
         if(logger.isDebugEnabled()) {
            logger.debug("{} operations timed out in the last {} msecs:{}{}", new Object[]{Long.valueOf(failedOperations.num()), Long.valueOf(elapsed), LINE_SEPARATOR, failedOperations.getLogMessage()});
         }

         return true;
      } else {
         return false;
      }
   }

   @VisibleForTesting
   boolean logSlowOperations(long now) {
      MonitoringTask.AggregatedOperations slowOperations = this.slowOperationsQueue.popOperations();
      if(!slowOperations.isEmpty()) {
         long elapsed = now - this.lastLogTime;
         noSpamLogger.info("Some operations were slow, details available at debug level (debug.log)", new Object[0]);
         if(logger.isDebugEnabled()) {
            logger.debug("{} operations were slow in the last {} msecs:{}{}", new Object[]{Long.valueOf(slowOperations.num()), Long.valueOf(elapsed), LINE_SEPARATOR, slowOperations.getLogMessage()});
         }

         return true;
      } else {
         return false;
      }
   }

   static {
      noSpamLogger = NoSpamLogger.getLogger(logger, 5L, TimeUnit.MINUTES);
      REPORT_INTERVAL_MS = Math.max(0, Integer.parseInt(System.getProperty("cassandra.monitoring_report_interval_ms", "5000")));
      MAX_OPERATIONS = Integer.parseInt(System.getProperty("cassandra.monitoring_max_operations", "50"));
      instance = make(REPORT_INTERVAL_MS, MAX_OPERATIONS);
   }

   private static final class SlowOperation extends MonitoringTask.Operation {
      SlowOperation(Monitor monitor, long failedAt) {
         super(monitor, failedAt);
      }

      public String getLogMessage() {
         return this.numTimesReported == 1?String.format("<%s>, time %d msec - slow timeout %d %s", new Object[]{this.name(), Long.valueOf(this.totalTime), Long.valueOf(this.monitor.slowQueryTimeoutMillis), this.monitor.isLocalOperation?"msec":"msec/cross-node"}):String.format("<%s>, was slow %d times: avg/min/max %d/%d/%d msec - slow timeout %d %s", new Object[]{this.name(), Integer.valueOf(this.numTimesReported), Long.valueOf(this.totalTime / (long)this.numTimesReported), Long.valueOf(this.minTime), Long.valueOf(this.maxTime), Long.valueOf(this.monitor.slowQueryTimeoutMillis), this.monitor.isLocalOperation?"msec":"msec/cross-node"});
      }
   }

   private static final class FailedOperation extends MonitoringTask.Operation {
      FailedOperation(Monitor monitor, long failedAt) {
         super(monitor, failedAt);
      }

      public String getLogMessage() {
         return this.numTimesReported == 1?String.format("<%s>, total time %d msec, timeout %d %s", new Object[]{this.name(), Long.valueOf(this.totalTime), Long.valueOf(this.monitor.timeoutMillis), this.monitor.isLocalOperation?"msec":"msec/cross-node"}):String.format("<%s> timed out %d times, avg/min/max %d/%d/%d msec, timeout %d %s", new Object[]{this.name(), Integer.valueOf(this.numTimesReported), Long.valueOf(this.totalTime / (long)this.numTimesReported), Long.valueOf(this.minTime), Long.valueOf(this.maxTime), Long.valueOf(this.monitor.timeoutMillis), this.monitor.isLocalOperation?"msec":"msec/cross-node"});
      }
   }

   protected abstract static class Operation {
      final Monitor monitor;
      int numTimesReported;
      long totalTime;
      long maxTime;
      long minTime;
      private String name;

      Operation(Monitor monitor, long failedAt) {
         this.monitor = monitor;
         this.numTimesReported = 1;
         this.totalTime = failedAt - monitor.operationCreationTimeMillis;
         this.minTime = this.totalTime;
         this.maxTime = this.totalTime;
      }

      public String name() {
         if(this.name == null) {
            this.name = this.monitor.operation.name();
         }

         return this.name;
      }

      void add(MonitoringTask.Operation operation) {
         ++this.numTimesReported;
         this.totalTime += operation.totalTime;
         this.maxTime = Math.max(this.maxTime, operation.maxTime);
         this.minTime = Math.min(this.minTime, operation.minTime);
      }

      public abstract String getLogMessage();
   }

   private static final class AggregatedOperations {
      private final Map<String, MonitoringTask.Operation> operations;
      private final long numDropped;

      AggregatedOperations(Map<String, MonitoringTask.Operation> operations, long numDropped) {
         this.operations = operations;
         this.numDropped = numDropped;
      }

      public boolean isEmpty() {
         return this.operations.isEmpty() && this.numDropped == 0L;
      }

      public long num() {
         return (long)this.operations.size() + this.numDropped;
      }

      String getLogMessage() {
         if(this.isEmpty()) {
            return "";
         } else {
            StringBuilder ret = new StringBuilder();
            this.operations.values().forEach((o) -> {
               addOperation(ret, o);
            });
            if(this.numDropped > 0L) {
               ret.append(MonitoringTask.LINE_SEPARATOR).append("... (").append(this.numDropped).append(" were dropped)");
            }

            return ret.toString();
         }
      }

      private static void addOperation(StringBuilder ret, MonitoringTask.Operation operation) {
         if(ret.length() > 0) {
            ret.append(MonitoringTask.LINE_SEPARATOR);
         }

         ret.append(operation.getLogMessage());
      }
   }

   private static final class OperationsQueue {
      private final int maxOperations;
      private final BlockingQueue<MonitoringTask.Operation> queue;
      private final AtomicLong numDroppedOperations;

      OperationsQueue(int maxOperations) {
         this.maxOperations = maxOperations;
         this.queue = (BlockingQueue)(maxOperations > 0?new ArrayBlockingQueue(maxOperations):new LinkedBlockingQueue());
         this.numDroppedOperations = new AtomicLong();
      }

      private void offer(MonitoringTask.Operation operation) {
         if(this.maxOperations != 0) {
            if(!this.queue.offer(operation)) {
               this.numDroppedOperations.incrementAndGet();
            }

         }
      }

      private MonitoringTask.AggregatedOperations popOperations() {
         HashMap operations = new HashMap();

         MonitoringTask.Operation operation;
         while((operation = (MonitoringTask.Operation)this.queue.poll()) != null) {
            MonitoringTask.Operation existing = (MonitoringTask.Operation)operations.get(operation.name());
            if(existing != null) {
               existing.add(operation);
            } else {
               operations.put(operation.name(), operation);
            }
         }

         return new MonitoringTask.AggregatedOperations(operations, this.numDroppedOperations.getAndSet(0L));
      }
   }
}
