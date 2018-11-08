package com.datastax.bdp.db.nodesync;

import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.exceptions.InternalRequestExecutionException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.units.SizeUnit;
import org.apache.cassandra.utils.units.Units;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class NodeSyncTracing {
   private static final Logger logger = LoggerFactory.getLogger(NodeSyncTracing.class);
   private final AtomicLong idGenerator = new AtomicLong();
   private volatile TraceState state;
   private volatile TracingLevel level;
   private volatile Future<?> timeouterFuture;
   private volatile Set<TableId> includeTables;

   NodeSyncTracing() {
   }

   synchronized UUID enable(TracingOptions options) {
      if(this.state != null) {
         throw new NodeSyncTracing.NodeSyncTracingAlreadyEnabledException(this.state.sessionId);
      } else {
         assert this.timeouterFuture == null;

         this.idGenerator.set(0L);
         this.level = options.level;
         this.includeTables = options.tables;

         UUID var3;
         try {
            Tracing.instance.newSession(options.id, Tracing.TraceType.NODESYNC);
            Tracing.instance.begin("NodeSync Tracing", Collections.emptyMap());
            this.state = Tracing.instance.get();
            if(options.timeoutSec > 0L) {
               this.timeouterFuture = ScheduledExecutors.nonPeriodicTasks.schedule(() -> {
                  this.disable(true);
               }, options.timeoutSec, TimeUnit.SECONDS);
            }

            String durationStr = options.timeoutSec <= 0L?"":" for " + Units.toString(options.timeoutSec, TimeUnit.SECONDS);
            this.state.trace("Starting NodeSync tracing on {}{}", FBUtilities.getBroadcastAddress(), durationStr);
            logger.info("Starting NodeSync tracing{}. Tracing session ID is {}", durationStr, options.id);
            var3 = options.id;
         } finally {
            Tracing.instance.set((TraceState)null);
         }

         return var3;
      }
   }

   synchronized Optional<UUID> currentTracingSession() {
      return this.state == null?Optional.empty():Optional.of(this.state.sessionId);
   }

   synchronized boolean isEnabled() {
      return this.state != null;
   }

   void disable() {
      this.disable(false);
   }

   private synchronized void disable(boolean wasAutomatic) {
      if(this.state != null) {
         if(this.timeouterFuture != null) {
            this.timeouterFuture.cancel(true);
         }

         this.timeouterFuture = null;
         TraceState current = this.state;
         this.state = null;
         current.trace("Stopped NodeSync tracing on {}", (Object)FBUtilities.getBroadcastAddress());
         Tracing.instance.set(current);
         Tracing.instance.stopSession();
         Tracing.instance.set((TraceState)null);
         logger.info("Stopped NodeSync tracing{}", wasAutomatic?" (timeout on tracing expired)":"");
      }
   }

   private TraceState stateFor(Segment segment) {
      TraceState current = this.state;
      return current == null?null:(this.includeTables != null && !this.includeTables.contains(segment.table.id)?null:current);
   }

   NodeSyncTracing.SegmentTracing startContinuous(Segment segment, String details) {
      TraceState current = this.stateFor(segment);
      if(current == null) {
         return NodeSyncTracing.SegmentTracing.NO_TRACING;
      } else {
         long id = this.idGenerator.getAndIncrement();
         current.trace("[#{}] Starting validation on {} of {} ({})", new Object[]{Long.valueOf(id), segment.range, segment.table, details});
         return new NodeSyncTracing.SegmentTracing(current, id, this.level);
      }
   }

   void skipContinuous(Segment segment, String reason) {
      TraceState current = this.stateFor(segment);
      if(current != null) {
         current.trace("[#-] Skipping {} of {}, {}", new Object[]{segment.range, segment.table, reason});
      }
   }

   NodeSyncTracing.SegmentTracing forUserValidation(Segment segment) {
      TraceState current = this.stateFor(segment);
      if(current == null) {
         return NodeSyncTracing.SegmentTracing.NO_TRACING;
      } else {
         long id = this.idGenerator.getAndIncrement();
         current.trace("Starting user validation on segment {} (id: #{})", segment, Long.valueOf(id));
         return new NodeSyncTracing.SegmentTracing(current, id, this.level);
      }
   }

   void trace(String message, Object... args) {
      TraceState current = this.state;
      if(current != null) {
         current.trace(message, args);
      }
   }

   static class NodeSyncTracingAlreadyEnabledException extends InternalRequestExecutionException {
      NodeSyncTracingAlreadyEnabledException(UUID sessionId) {
         super(RequestFailureReason.NODESYNC_TRACING_ALREADY_ENABLED, String.format("Tracing for NodeSync is already enabled (session id: %s)", new Object[]{sessionId}));
      }
   }

   static class SegmentTracing {
      @VisibleForTesting
      static final NodeSyncTracing.SegmentTracing NO_TRACING = new NodeSyncTracing.SegmentTracing();
      private final TraceState state;
      private final String id;
      private final TracingLevel level;
      private final long startTime;

      private SegmentTracing() {
         this.startTime = NodeSyncHelpers.time().currentTimeMillis();
         this.state = null;
         this.id = null;
         this.level = TracingLevel.LOW;
      }

      private SegmentTracing(TraceState state, long id, TracingLevel level) {
         this.startTime = NodeSyncHelpers.time().currentTimeMillis();

         assert state != null;

         this.state = state;
         this.id = String.format("[#%d] ", new Object[]{Long.valueOf(id)});
         this.level = level;
      }

      boolean isEnabled() {
         return this.state != null;
      }

      void trace(String message) {
         if(this.level != TracingLevel.LOW && this.state != null) {
            this.state.trace(this.id + message);
         }

      }

      void trace(String message, Object arg) {
         if(this.level != TracingLevel.LOW && this.state != null) {
            this.state.trace(this.id + message, arg);
         }

      }

      void trace(String message, Object... args) {
         if(this.level != TracingLevel.LOW && this.state != null) {
            this.state.trace(this.id + message, args);
         }

      }

      void onSegmentCompletion(ValidationOutcome outcome, ValidationMetrics metrics) {
         if(this.state != null) {
            long durationMs = NodeSyncHelpers.time().currentTimeMillis() - this.startTime;
            this.state.trace(this.id + "Completed validation ({}) in {}: validated {} and repaired {}", new Object[]{outcome, Units.toString(durationMs, TimeUnit.MILLISECONDS), Units.toString(metrics.dataValidated(), SizeUnit.BYTES), Units.toString(metrics.dataRepaired(), SizeUnit.BYTES)});
         }
      }
   }
}
