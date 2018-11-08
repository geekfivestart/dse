package org.apache.cassandra.tracing;

import com.google.common.annotations.VisibleForTesting;
import io.reactivex.Completable;
import io.reactivex.CompletableOnSubscribe;
import io.reactivex.functions.Function;
import java.net.InetAddress;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.WrappedRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TraceStateImpl extends TraceState {
   private static final Logger logger = LoggerFactory.getLogger(TraceStateImpl.class);
   @VisibleForTesting
   public static int WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS = Integer.parseInt(System.getProperty("cassandra.wait_for_tracing_events_timeout_secs", "0"));
   private final Set<Future<Void>> pendingFutures = ConcurrentHashMap.newKeySet();

   public TraceStateImpl(InetAddress coordinator, UUID sessionId, Tracing.TraceType traceType) {
      super(coordinator, sessionId, traceType);
   }

   protected void traceImpl(String message) {
      String threadName = Thread.currentThread().getName();
      int elapsed = this.elapsed();
      this.executeMutation(TraceKeyspace.makeEventMutation(this.sessionIdBytes, message, elapsed, threadName, this.ttl));
      if(logger.isTraceEnabled()) {
         logger.trace("Adding <{}> to trace events", message);
      }

   }

   protected Completable waitForPendingEvents() {
      if(WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS <= 0) {
         return Completable.complete();
      } else {
         CompletableFuture<Void> fut = CompletableFuture.allOf((CompletableFuture[])this.pendingFutures.toArray(new CompletableFuture[0]));
         return Completable.create((subscriber) -> {
            fut.whenComplete((result, error) -> {
               if(error != null) {
                  subscriber.onError(error);
               } else {
                  subscriber.onComplete();
               }

            });
         }).timeout((long)WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS, TimeUnit.SECONDS).onErrorResumeNext((ex) -> {
            if(ex instanceof TimeoutException) {
               if(logger.isTraceEnabled()) {
                  logger.trace("Failed to wait for tracing events to complete in {} seconds", Integer.valueOf(WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS));
               }
            } else {
               JVMStabilityInspector.inspectThrowable(ex);
               logger.error("Got exception whilst waiting for tracing events to complete", ex);
            }

            return Completable.complete();
         });
      }
   }

   void executeMutation(final Mutation mutation) {
      CompletableFuture<Void> fut = CompletableFuture.runAsync(new WrappedRunnable() {
         protected void runMayThrow() {
            TraceStateImpl.mutateWithCatch(mutation);
         }
      }, StageManager.tracingExecutor);
      boolean ret = this.pendingFutures.add(fut);
      if(!ret) {
         logger.warn("Failed to insert pending future, tracing synchronization may not work");
      }

   }

   static void mutateWithCatch(Mutation mutation) {
      try {
         StorageProxy.mutate(Collections.singletonList(mutation), ConsistencyLevel.ANY, System.nanoTime()).blockingGet();
      } catch (OverloadedException var2) {
         Tracing.logger.warn("Too many nodes are overloaded to save trace events");
      } catch (Throwable var3) {
         JVMStabilityInspector.inspectThrowable(var3);
         logger.error("Could not apply tracing mutation {}", mutation, var3);
      }

   }
}
