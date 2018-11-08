package com.datastax.bdp.db.nodesync;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Longs;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.schema.NodeSyncParams;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.units.Units;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ContinuousValidationProposer extends ValidationProposer {
   private static final long RETRY_DELAY_MS;
   private static final Logger logger;
   private static final NoSpamLogger noSpamLogger;
   private final Consumer<ContinuousValidationProposer.Proposal> proposalConsumer;
   private volatile boolean cancelled;

   ContinuousValidationProposer(TableState state, Consumer<ContinuousValidationProposer.Proposal> proposalConsumer) {
      super(state);
      this.proposalConsumer = proposalConsumer;
   }

   ContinuousValidationProposer start() {
      this.generateNextProposal();
      return this;
   }

   @VisibleForTesting
   void generateNextProposal() {
      if(!this.cancelled) {
         TableState.Ref ref = this.state.nextSegmentToValidate();
         if(ref.segmentStateAtCreation().isLocallyLocked()) {
            ScheduledExecutors.nonPeriodicTasks.schedule(this::generateNextProposal, RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
         } else {
            ContinuousValidationProposer.Proposal next = new ContinuousValidationProposer.Proposal(ref);
            long now = NodeSyncHelpers.time().currentTimeMillis();
            if(next.minTimeForSubmission() > now) {
               ScheduledExecutors.nonPeriodicTasks.schedule(() -> {
                  this.proposalConsumer.accept(next);
               }, next.minTimeForSubmission() - now, TimeUnit.MILLISECONDS);
            } else {
               this.proposalConsumer.accept(next);
            }

         }
      }
   }

   public boolean cancel() {
      this.cancelled = true;
      return true;
   }

   public boolean isCancelled() {
      return this.cancelled;
   }

   public String toString() {
      return String.format("Continuous validations of %s", new Object[]{this.table()});
   }

   static {
      RETRY_DELAY_MS = TimeUnit.SECONDS.toMillis(5L);
      logger = LoggerFactory.getLogger(ContinuousValidationProposer.class);
      noSpamLogger = NoSpamLogger.getLogger(logger, 10L, TimeUnit.MINUTES);
   }

   class Proposal extends ValidationProposal implements Comparable<ContinuousValidationProposer.Proposal> {
      private final long priority;
      private final long minTimeForSubmission;

      private Proposal(TableState.Ref segmentRef) {
         super(segmentRef);
         this.priority = this.stateAtProposal().priority();
         this.minTimeForSubmission = this.minTimeForNextValidation(this.stateAtProposal());
      }

      private SegmentState stateAtProposal() {
         return this.segmentRef.segmentStateAtCreation();
      }

      private long minTimeForNextValidation(SegmentState state) {
         if(NodeSyncService.MIN_VALIDATION_INTERVAL_MS < 0L) {
            return -9223372036854775808L;
         } else {
            if(state.deadlineTargetMs() <= NodeSyncService.MIN_VALIDATION_INTERVAL_MS) {
               boolean minValidationIsHigh = NodeSyncService.MIN_VALIDATION_INTERVAL_MS > TimeUnit.HOURS.toMillis(10L);
               ContinuousValidationProposer.noSpamLogger.warn("NodeSync '{}' setting on {} is {} which is lower than the {} value ({}): this mean that deadline cannot be achieved, at least on this node, and indicate a misconfiguration. {}", new Object[]{NodeSyncParams.Option.DEADLINE_TARGET_SEC, state.segment().table, Units.toString(state.deadlineTargetMs(), TimeUnit.MILLISECONDS), "dse.nodesync.min_validation_interval_ms", Units.toString(NodeSyncService.MIN_VALIDATION_INTERVAL_MS, TimeUnit.MILLISECONDS), minValidationIsHigh?"The custom value set for dse.nodesync.min_validation_interval_ms seems unwisely high":"That '" + NodeSyncParams.Option.DEADLINE_TARGET_SEC + "' value seems unwisely low value"});
            }

            return state.lastValidationTimeMs() + NodeSyncService.MIN_VALIDATION_INTERVAL_MS;
         }
      }

      private long minTimeForSubmission() {
         return this.minTimeForSubmission;
      }

      Validator activate() {
         TableState.Ref.Status status = this.segmentRef.checkStatus();
         if(status.isUpToDate()) {
            NodeSyncTracing.SegmentTracing tracing = ContinuousValidationProposer.this.service().tracing().startContinuous(this.segment(), this.stateAtProposal().toTraceString());
            ValidationLifecycle lifecycle = ValidationLifecycle.createAndStart(this.segmentRef, tracing);
            ContinuousValidationProposer.this.generateNextProposal();
            return Validator.create(lifecycle);
         } else {
            if(ContinuousValidationProposer.this.service().tracing().isEnabled()) {
               ContinuousValidationProposer.this.service().tracing().skipContinuous(this.segment(), status.toString());
            }

            if(status.isRemotelyLocked() && this.stateAtProposal().isRemotelyLocked()) {
               ScheduledExecutors.nonPeriodicTasks.schedule(ContinuousValidationProposer.this::generateNextProposal, ContinuousValidationProposer.RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
            } else {
               ContinuousValidationProposer.this.generateNextProposal();
            }

            return null;
         }
      }

      public int compareTo(ContinuousValidationProposer.Proposal other) {
         return this.equals(other)?0:Longs.compare(this.priority, other.priority);
      }

      public String toString() {
         long now = NodeSyncHelpers.time().currentTimeMillis();
         if(this.minTimeForSubmission <= now) {
            return this.stateAtProposal().toString();
         } else {
            String delayToSubmission = Units.toString(this.minTimeForSubmission - now, TimeUnit.MILLISECONDS);
            return String.format("%s (to be submitted in %s)", new Object[]{this.stateAtProposal(), delayToSubmission});
         }
      }
   }
}
