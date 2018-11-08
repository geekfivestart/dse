package com.datastax.bdp.db.nodesync;

import com.google.common.annotations.VisibleForTesting;
import io.netty.util.concurrent.FastThreadLocalThread;
import io.reactivex.Scheduler;
import io.reactivex.Scheduler.Worker;
import io.reactivex.schedulers.Schedulers;
import java.util.Iterator;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCRunnable;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.concurrent.TracingAwareExecutor;
import org.apache.cassandra.concurrent.TracingAwareExecutorService;
import org.apache.cassandra.config.NodeSyncConfig;
import org.apache.cassandra.utils.collection.History;
import org.apache.cassandra.utils.units.RateUnit;
import org.apache.cassandra.utils.units.SizeUnit;
import org.apache.cassandra.utils.units.Units;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ValidationExecutor implements Validator.PageProcessingListener {
   private static final Logger logger = LoggerFactory.getLogger(ValidationExecutor.class);
   private static final long DEFAULT_CONTROLLER_INTERVAL_SEC;
   private final AtomicReference<ValidationExecutor.State> state;
   private final ValidationScheduler scheduler;
   private final NodeSyncConfig config;
   private final DebuggableThreadPoolExecutor validationExecutor;
   private final StagedScheduler wrappingScheduler;
   private final ScheduledExecutorService updaterExecutor;
   private volatile ValidationExecutor.Controller controller;
   private final AtomicInteger inFlightValidations;
   private volatile int maxInFlightValidations;
   private final Set<Validator> inFlightValidators;
   private final CompletableFuture<Void> shutdownFuture;
   private final AtomicLong processingWaitTimeNanos;
   private final AtomicLong limiterWaitTimeMicros;
   private final AtomicLong dataValidatedBytes;
   private final AtomicLong blockedOnNewTaskTimeNanos;
   private final ConcurrentMap<Thread, Long> waitingOnTaskThreads;
   private final long controllerIntervalMs;

   ValidationExecutor(ValidationScheduler scheduler, NodeSyncConfig config) {
      this(scheduler, config, TimeUnit.SECONDS.toMillis(DEFAULT_CONTROLLER_INTERVAL_SEC));
   }

   @VisibleForTesting
   ValidationExecutor(ValidationScheduler scheduler, NodeSyncConfig config, long controllerIntervalMs) {
      this.state = new AtomicReference(ValidationExecutor.State.CREATED);
      this.updaterExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("NodeSyncController"));
      this.inFlightValidations = new AtomicInteger();
      this.inFlightValidators = ConcurrentHashMap.newKeySet();
      this.shutdownFuture = new CompletableFuture();
      this.processingWaitTimeNanos = new AtomicLong();
      this.limiterWaitTimeMicros = new AtomicLong();
      this.dataValidatedBytes = new AtomicLong();
      this.blockedOnNewTaskTimeNanos = new AtomicLong();
      this.waitingOnTaskThreads = new ConcurrentHashMap();
      this.scheduler = scheduler;
      this.validationExecutor = DebuggableThreadPoolExecutor.createWithFixedPoolSize((ThreadFactory)(new ValidationExecutor.ValidationThread.Factory(null)), config.getMinThreads());
      this.wrappingScheduler = new ValidationExecutor.NodeSyncStagedExecutor(null);
      this.config = config;
      this.maxInFlightValidations = config.getMinInflightValidations();
      this.controllerIntervalMs = controllerIntervalMs;
   }

   StagedScheduler asScheduler() {
      return this.wrappingScheduler;
   }

   TracingAwareExecutorService asExecutor() {
      return this.validationExecutor;
   }

   @VisibleForTesting
   History<ValidationExecutor.Action> controllerHistory() {
      if(this.controller == null) {
         throw new IllegalStateException("The executor is not started");
      } else {
         return this.controller.history;
      }
   }

   @VisibleForTesting
   Set<Validator> inFlightValidators() {
      return this.inFlightValidators;
   }

   @VisibleForTesting
   long lastMaxedOutWarn() {
      if(this.controller == null) {
         throw new IllegalStateException("The executor is not started");
      } else {
         return this.controller.lastMaxedOutWarn;
      }
   }

   void start() {
      if(!this.state.compareAndSet(ValidationExecutor.State.CREATED, ValidationExecutor.State.RUNNING)) {
         if(((ValidationExecutor.State)this.state.get()).isShutdown()) {
            throw new IllegalStateException("Cannot restart a stopped ValidationExecutor");
         }
      } else {
         this.controller = new ValidationExecutor.Controller(this.controllerIntervalMs, null);
         this.controller.updateValues();

         for(int i = 0; i < this.maxInFlightValidations; ++i) {
            this.submitNewValidation();
         }

         this.updaterExecutor.scheduleAtFixedRate(this.controller, this.controllerIntervalMs, this.controllerIntervalMs, TimeUnit.MILLISECONDS);
      }
   }

   CompletableFuture<Void> shutdown(boolean interruptValidations) {
      ValidationExecutor.State current;
      do {
         current = (ValidationExecutor.State)this.state.get();
         if(current.isShutdown()) {
            return this.shutdownFuture;
         }
      } while(!this.state.compareAndSet(current, interruptValidations?ValidationExecutor.State.HARD_STOPPED:ValidationExecutor.State.SOFT_STOPPED));

      if(this.scheduler != null) {
         this.scheduler.shutdown();
      }

      if(interruptValidations) {
         this.inFlightValidators.forEach((v) -> {
            v.cancel("Shutting down NodeSync forcefully");
         });
      }

      if(this.inFlightValidations.get() == 0) {
         this.shutdownFuture.complete((Object)null);
      }

      this.updaterExecutor.shutdown();
      return this.shutdownFuture;
   }

   public boolean isShutdown() {
      return ((ValidationExecutor.State)this.state.get()).isShutdown();
   }

   private int getValidationPermit() {
      int current;
      do {
         current = this.inFlightValidations.get();
         if(current + 1 > this.maxInFlightValidations) {
            return 0;
         }
      } while(!this.inFlightValidations.compareAndSet(current, current + 1));

      return current + 1;
   }

   private void returnValidationPermit() {
      int value = this.inFlightValidations.decrementAndGet();
      if(value < 0) {
         logger.warn("More permit for NodeSync validations granted than returned, this is a bug that should be reported as such. However, if NodeSync is still running properly (based on metrics), this has likely little to no practical impact.");
      }

      if(value == 0 && this.isShutdown()) {
         this.shutdownFuture.complete((Object)null);
      }

   }

   private void submitNewValidation() {
      if(!this.isShutdown()) {
         int permitNr = this.getValidationPermit();
         if(permitNr > 0) {
            this.validationExecutor.submit(() -> {
               try {
                  Thread currentThread = Thread.currentThread();
                  boolean blockUntilNextValidation = permitNr == 1;
                  Validator validator = this.getNextValidation(false);
                  if(validator == null && blockUntilNextValidation) {
                     this.waitingOnTaskThreads.putIfAbsent(currentThread, Long.valueOf(System.nanoTime()));
                     validator = this.getNextValidation(true);
                  }

                  Long startTimeNanos;
                  if(validator != null) {
                     startTimeNanos = (Long)this.waitingOnTaskThreads.remove(currentThread);
                     if(startTimeNanos != null) {
                        this.blockedOnNewTaskTimeNanos.addAndGet(System.nanoTime() - startTimeNanos.longValue());
                     }

                     this.submit(validator);
                  } else {
                     startTimeNanos = (Long)this.waitingOnTaskThreads.put(currentThread, Long.valueOf(System.nanoTime()));
                     if(startTimeNanos != null) {
                        this.blockedOnNewTaskTimeNanos.addAndGet(System.nanoTime() - startTimeNanos.longValue());
                     }

                     this.returnValidationPermit();
                     ScheduledExecutors.scheduledTasks.schedule(this::submitNewValidation, 100L, TimeUnit.MILLISECONDS);
                  }
               } catch (ValidationScheduler.ShutdownException var6) {
                  assert this.isShutdown();

                  this.returnValidationPermit();
               } catch (Exception var7) {
                  logger.error("Unexpected error submitting new validation to NodeSync executor. This shouldn't happen and should be reported but unless this happens repeatedly, this shouldn't prevent NodeSync general progress", var7);
                  this.returnValidationPermit();
                  ScheduledExecutors.scheduledTasks.schedule(this::submitNewValidation, 100L, TimeUnit.MILLISECONDS);
               }

            });
         }

      }
   }

   @VisibleForTesting
   protected Validator getNextValidation(boolean blockUntilAvailable) {
      return this.scheduler.getNextValidation(blockUntilAvailable);
   }

   private void submit(Validator validator) {
      if(this.isShutdown()) {
         validator.cancel("NodeSync has been shutdown");
         this.returnValidationPermit();
      } else {
         this.inFlightValidators.add(validator);
         validator.executeOn(this).whenComplete((v, e) -> {
            this.inFlightValidators.remove(validator);
            this.onValidationDone();
            if(e != null && !(e instanceof CancellationException)) {
               logger.error("Unexpected error reported by NodeSync validator of table {}. This shouldn't happen and should be reported, but shouldn't have impact outside of the failure of that particular segment validation", validator.segment().table, e);
            }

         });
      }
   }

   public void onNewPage() {
      this.waitingOnTaskThreads.remove(Thread.currentThread());
   }

   public void onPageComplete(long processedBytes, long waitedOnLimiterMicros, long waitedForProcessingNanos) {
      this.dataValidatedBytes.addAndGet(processedBytes);
      this.limiterWaitTimeMicros.addAndGet(waitedOnLimiterMicros);
      this.processingWaitTimeNanos.addAndGet(waitedForProcessingNanos);
      this.waitingOnTaskThreads.remove(Thread.currentThread());
   }

   private void onValidationDone() {
      this.returnValidationPermit();
      this.submitNewValidation();
   }

   static {
      DEFAULT_CONTROLLER_INTERVAL_SEC = Long.getLong("dse.nodesync.controller_update_interval_sec", TimeUnit.MINUTES.toSeconds(5L)).longValue();
   }

   private static class ValidationThread extends FastThreadLocalThread {
      private ValidationThread(String name, Runnable runnable) {
         super((ThreadGroup)null, NamedThreadFactory.threadLocalDeallocator(runnable), name);
         this.setDaemon(true);
      }

      private static class Factory implements ThreadFactory {
         private final AtomicInteger n;

         private Factory() {
            this.n = new AtomicInteger(1);
         }

         public Thread newThread(Runnable runnable) {
            return new ValidationExecutor.ValidationThread("NodeSync-" + this.n.getAndIncrement(), runnable, null);
         }
      }
   }

   private class NodeSyncStagedExecutor extends StagedScheduler {
      private final Scheduler scheduler;

      private NodeSyncStagedExecutor() {
         this.scheduler = Schedulers.from(ValidationExecutor.this.validationExecutor);
      }

      public boolean isOnScheduler(Thread thread) {
         return thread instanceof ValidationExecutor.ValidationThread;
      }

      public int metricsCoreId() {
         return TPC.getNumCores();
      }

      public void enqueue(TPCRunnable runnable) {
         ValidationExecutor.this.validationExecutor.execute(runnable);
      }

      public Worker createWorker() {
         return this.scheduler.createWorker();
      }

      public TracingAwareExecutor forTaskType(TPCTaskType type) {
         return ValidationExecutor.this.validationExecutor;
      }
   }

   private static class DiffValue {
      private long previousTotal;
      private long currentDiff;

      private DiffValue() {
      }

      private void update(long currentTotal) {
         this.currentDiff = currentTotal - this.previousTotal;
         this.previousTotal = currentTotal;
      }

      private void addToCurrentDiff(long value) {
         this.currentDiff += value;
      }

      private long currentDiff() {
         return this.currentDiff;
      }
   }

   private class Controller implements Runnable {
      private final long threadWaitTimeThresholdMs;
      private final long limiterWaitTimeThresholdMs;
      private final long blockedOnNewTaskThresholdMs;
      private long lastTickNanos;
      private long lastIntervalMs;
      private final ValidationExecutor.DiffValue processingWaitTimeMsDiff;
      private final ValidationExecutor.DiffValue limiterWaitTimeMsDiff;
      private final ValidationExecutor.DiffValue dataValidatedBytesDiff;
      private final ValidationExecutor.DiffValue blockedOnNewTaskMsDiff;
      private final History<ValidationExecutor.Action> history;
      private long lastMaxedOutWarn;

      private Controller(long controllerIntervalMs) {
         this.lastTickNanos = System.nanoTime();
         this.processingWaitTimeMsDiff = new ValidationExecutor.DiffValue(null);
         this.limiterWaitTimeMsDiff = new ValidationExecutor.DiffValue(null);
         this.dataValidatedBytesDiff = new ValidationExecutor.DiffValue(null);
         this.blockedOnNewTaskMsDiff = new ValidationExecutor.DiffValue(null);
         this.history = new History(12);
         this.lastMaxedOutWarn = -1L;
         this.threadWaitTimeThresholdMs = 10L * controllerIntervalMs / 100L;
         this.limiterWaitTimeThresholdMs = 5L * controllerIntervalMs / 100L;
         this.blockedOnNewTaskThresholdMs = 10L * controllerIntervalMs / 100L;
      }

      private boolean hasRecentUnsuccessfulDecrease() {
         Iterator iter = this.history.iterator();

         while(iter.hasNext()) {
            ValidationExecutor.Action action = (ValidationExecutor.Action)iter.next();
            if(action.isDecrease()) {
               break;
            }

            if(action.isIncrease()) {
               return iter.hasNext() && ((ValidationExecutor.Action)iter.next()).isDecrease();
            }
         }

         return false;
      }

      private boolean hasSignificantProcessingWaitTimeSinceLastCheck() {
         return this.processingWaitTimeMsDiff.currentDiff() > this.threadWaitTimeThresholdMs;
      }

      private boolean hasSignificantLimiterWaitTimeSinceLastCheck() {
         return this.limiterWaitTimeMsDiff.currentDiff() > this.limiterWaitTimeThresholdMs;
      }

      private long perThreadAvgBlockTime() {
         return this.blockedOnNewTaskMsDiff.currentDiff() / (long)ValidationExecutor.this.validationExecutor.getCorePoolSize();
      }

      private int threadAvgOccupationPercentage() {
         long timeOccupied = this.lastIntervalMs - this.perThreadAvgBlockTime();
         float occupationPercentage = 100.0F * ((float)timeOccupied / (float)this.lastIntervalMs);
         int value = Math.round(occupationPercentage);
         return value < 0?0:(value > 100?100:value);
      }

      private boolean hasSignificantBlockOnNewTaskSinceLastCheck() {
         return this.perThreadAvgBlockTime() > this.blockedOnNewTaskThresholdMs;
      }

      private boolean canIncreaseThreads() {
         int count = ValidationExecutor.this.validationExecutor.getCorePoolSize();
         return count < ValidationExecutor.this.config.getMaxThreads() && count < ValidationExecutor.this.maxInFlightValidations;
      }

      private boolean canIncreaseInflightValidations() {
         return ValidationExecutor.this.maxInFlightValidations < ValidationExecutor.this.config.getMaxInflightValidations();
      }

      private boolean canDecreaseThreads() {
         return ValidationExecutor.this.validationExecutor.getCorePoolSize() > ValidationExecutor.this.config.getMinThreads();
      }

      private boolean canDecreaseInflightValidations() {
         return ValidationExecutor.this.maxInFlightValidations > ValidationExecutor.this.config.getMinInflightValidations() && ValidationExecutor.this.maxInFlightValidations > ValidationExecutor.this.validationExecutor.getCorePoolSize();
      }

      private ValidationExecutor.Action pickDecreaseAction() {
         return !this.canDecreaseInflightValidations()?(this.canDecreaseThreads()?ValidationExecutor.Action.DECREASE_THREADS:ValidationExecutor.Action.MINED_OUT):(!this.canDecreaseThreads()?ValidationExecutor.Action.DECREASE_INFLIGHT_VALIDATIONS:(this.hasSignificantProcessingWaitTimeSinceLastCheck()?ValidationExecutor.Action.DECREASE_INFLIGHT_VALIDATIONS:ValidationExecutor.Action.DECREASE_THREADS));
      }

      private ValidationExecutor.Action pickIncreaseAction() {
         return !this.canIncreaseInflightValidations()?(this.canIncreaseThreads()?ValidationExecutor.Action.INCREASE_THREADS:ValidationExecutor.Action.MAXED_OUT):(!this.canIncreaseThreads()?ValidationExecutor.Action.INCREASE_INFLIGHT_VALIDATIONS:(this.hasSignificantProcessingWaitTimeSinceLastCheck()?ValidationExecutor.Action.INCREASE_THREADS:ValidationExecutor.Action.INCREASE_INFLIGHT_VALIDATIONS));
      }

      private void updateValues() {
         long nowNanos = System.nanoTime();
         this.dataValidatedBytesDiff.update(ValidationExecutor.this.dataValidatedBytes.get());
         this.processingWaitTimeMsDiff.update(TimeUnit.NANOSECONDS.toMillis(ValidationExecutor.this.processingWaitTimeNanos.get()));
         this.limiterWaitTimeMsDiff.update(TimeUnit.MICROSECONDS.toMillis(ValidationExecutor.this.limiterWaitTimeMicros.get()));
         this.lastIntervalMs = TimeUnit.NANOSECONDS.toMillis(nowNanos - this.lastTickNanos);
         this.lastTickNanos = nowNanos;
         this.blockedOnNewTaskMsDiff.update(TimeUnit.NANOSECONDS.toMillis(ValidationExecutor.this.blockedOnNewTaskTimeNanos.get()));
         Iterator threads = ValidationExecutor.this.waitingOnTaskThreads.entrySet().iterator();

         while(threads.hasNext()) {
            Entry<Thread, Long> thread = (Entry)threads.next();
            if(((Thread)thread.getKey()).isAlive()) {
               long totalBlockedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - ((Long)thread.getValue()).longValue());
               this.blockedOnNewTaskMsDiff.addToCurrentDiff(Math.min(totalBlockedMs, this.lastIntervalMs));
            } else {
               threads.remove();
            }
         }

      }

      public void run() {
         if(!ValidationExecutor.this.isShutdown()) {
            this.updateValues();
            double targetRate = (double)ValidationExecutor.this.config.getRate().in(RateUnit.B_S);
            double recentRate = 1000.0D * (double)this.dataValidatedBytesDiff.currentDiff() / (double)this.lastIntervalMs;
            ValidationExecutor.Action action = ValidationExecutor.Action.DO_NOTHING;
            if(recentRate < targetRate * 0.95D) {
               if(this.hasSignificantBlockOnNewTaskSinceLastCheck()) {
                  action = this.pickDecreaseAction();
               } else {
                  action = this.pickIncreaseAction();
                  if(action == ValidationExecutor.Action.MAXED_OUT) {
                     this.maybeWarnOnMaxedOut(recentRate);
                  }
               }
            } else if(recentRate > targetRate * 1.05D) {
               ValidationExecutor.logger.debug("Recent effective rate {} is higher than the configured rate {}; this may temporarily happen while the server warm up but shouldn't happen in general. If you see this with some regularity, please report", Units.toString((long)recentRate, RateUnit.B_S), Units.toString((long)targetRate, RateUnit.B_S));
               action = this.pickDecreaseAction();
            } else if(!this.hasRecentUnsuccessfulDecrease() && this.hasSignificantLimiterWaitTimeSinceLastCheck()) {
               action = this.pickDecreaseAction();
            }

            if(ValidationExecutor.logger.isDebugEnabled()) {
               ValidationExecutor.logger.debug("NodeSync executor controller: recent rate={} (configured={}), {} thread(s) and {} maximum in-flight validation(s), ~{}% avg thread occupation: {}", new Object[]{Units.toString((long)recentRate, RateUnit.B_S), Units.toString((long)targetRate, RateUnit.B_S), Integer.valueOf(ValidationExecutor.this.validationExecutor.getCorePoolSize()), Integer.valueOf(ValidationExecutor.this.maxInFlightValidations), Integer.valueOf(this.threadAvgOccupationPercentage()), action});
            }

            switch(null.$SwitchMap$com$datastax$bdp$db$nodesync$ValidationExecutor$Action[action.ordinal()]) {
            case 1:
               ValidationExecutor.this.validationExecutor.setCorePoolSize(ValidationExecutor.this.validationExecutor.getCorePoolSize() + 1);
               break;
            case 2:
               ++ValidationExecutor.this.maxInFlightValidations;
               ValidationExecutor.this.submitNewValidation();
               break;
            case 3:
               ValidationExecutor.this.validationExecutor.setCorePoolSize(ValidationExecutor.this.validationExecutor.getCorePoolSize() - 1);
               break;
            case 4:
               --ValidationExecutor.this.maxInFlightValidations;
            }

            this.history.add(action);
            this.postRunCleanup();
         }
      }

      private void maybeWarnOnMaxedOut(double recentRate) {
         long now = NodeSyncHelpers.time().currentTimeMillis();
         if(this.lastMaxedOutWarn < 0L || now - this.lastMaxedOutWarn >= NodeSyncService.MIN_VALIDATION_INTERVAL_MS) {
            if(this.history.last() == ValidationExecutor.Action.MAXED_OUT || !this.history.isAtCapacity() || this.history.stream().filter((a) -> {
               return a == ValidationExecutor.Action.MAXED_OUT;
            }).count() > (long)(30 * this.history.size() / 100)) {
               this.lastMaxedOutWarn = now;
               ValidationExecutor.logger.warn("NodeSync doesn't seem to be able to sustain the configured rate (over the last {}, the effective rate was {} for a configured rate of {}) and this despite using {} threads and {} parallel range validations (maximums allowed). You may try to improve throughput by increasing the maximum allowed number of threads and/or parallel range validations with the understanding that this may result in NodeSync using more of the node resources. If doing so doesn't help, this suggests the configured rate cannot be sustained by NodeSync on the current hardware.", new Object[]{Units.toString(this.lastIntervalMs, TimeUnit.MILLISECONDS), Units.toString((long)recentRate, RateUnit.B_S), ValidationExecutor.this.config.getRate(), Integer.valueOf(ValidationExecutor.this.validationExecutor.getCorePoolSize()), Integer.valueOf(ValidationExecutor.this.maxInFlightValidations)});
            }
         }
      }

      private void postRunCleanup() {
         if(this.lastMaxedOutWarn >= 0L && this.history.isAtCapacity() && this.history.stream().noneMatch((a) -> {
            return a == ValidationExecutor.Action.MAXED_OUT;
         })) {
            this.lastMaxedOutWarn = -1L;
         }

      }

      public String toString() {
         return String.format("Interval: %s, processing wait time: %s, limiter wait time: %s, data validated; %s, blocked on new task: %s", new Object[]{Units.toString(this.lastIntervalMs, TimeUnit.MILLISECONDS), Units.toString(this.processingWaitTimeMsDiff.currentDiff, TimeUnit.MILLISECONDS), Units.toString(this.limiterWaitTimeMsDiff.currentDiff, TimeUnit.MILLISECONDS), Units.toString(this.dataValidatedBytesDiff.currentDiff, SizeUnit.BYTES), Units.toString(this.blockedOnNewTaskMsDiff.currentDiff, TimeUnit.MILLISECONDS)});
      }
   }

   @VisibleForTesting
   static enum Action {
      INCREASE_THREADS,
      INCREASE_INFLIGHT_VALIDATIONS,
      MAXED_OUT,
      DO_NOTHING,
      MINED_OUT,
      DECREASE_THREADS,
      DECREASE_INFLIGHT_VALIDATIONS;

      private Action() {
      }

      boolean isIncrease() {
         return this == INCREASE_THREADS || this == INCREASE_INFLIGHT_VALIDATIONS;
      }

      boolean isDecrease() {
         return this == DECREASE_THREADS || this == DECREASE_INFLIGHT_VALIDATIONS;
      }
   }

   private static enum State {
      CREATED,
      RUNNING,
      SOFT_STOPPED,
      HARD_STOPPED;

      private State() {
      }

      public boolean isShutdown() {
         return this == HARD_STOPPED || this == SOFT_STOPPED;
      }
   }
}
