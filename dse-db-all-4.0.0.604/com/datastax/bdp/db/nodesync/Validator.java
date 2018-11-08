package com.datastax.bdp.db.nodesync;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.RateLimiter;
import io.reactivex.functions.Function;
import java.net.InetAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import javax.annotation.Nullable;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.NodeSyncConfig;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.NodeSyncReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadContext;
import org.apache.cassandra.db.ReadReconciliationObserver;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.db.rows.FlowablePartitionBase;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestFailureException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.UnknownColumnException;
import org.apache.cassandra.exceptions.UnknownKeyspaceException;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ReadRepairDecision;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.flow.Threads;
import org.apache.cassandra.utils.units.SizeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Validator {
   private static final Logger logger = LoggerFactory.getLogger(Validator.class);
   private final ValidationLifecycle lifecycle;
   private final ValidationMetrics metrics = new ValidationMetrics();
   private final RateLimiter limiter;
   private final PageSize pageSize;
   protected final Validator.Observer observer;
   protected final int replicationFactor;
   private final Set<InetAddress> segmentReplicas;
   private final Validator.Future completionFuture = new Validator.Future();
   private CompletableFuture<Void> flowFuture;
   private final AtomicReference<Validator.State> state;
   private volatile ValidationOutcome validationOutcome;
   @Nullable
   private volatile Set<InetAddress> missingNodes;

   protected Validator(ValidationLifecycle lifecycle) {
      this.state = new AtomicReference(Validator.State.CREATED);
      this.validationOutcome = ValidationOutcome.FULL_IN_SYNC;
      this.lifecycle = lifecycle;
      NodeSyncConfig config = lifecycle.service().config();
      this.limiter = config.rateLimiter;
      this.pageSize = new PageSize(Ints.checkedCast(config.getPageSize(SizeUnit.BYTES)), PageSize.PageUnit.BYTES);
      this.observer = new Validator.Observer();
      AbstractReplicationStrategy replicationStrategy = Keyspace.open(this.table().keyspace).getReplicationStrategy();
      this.replicationFactor = replicationStrategy.getReplicationFactor();
      this.segmentReplicas = new HashSet(replicationStrategy.getNaturalEndpoints(this.range().left));
   }

   static Validator create(ValidationLifecycle lifecycle) {
      try {
         return new Validator(lifecycle);
      } catch (Exception var2) {
         lifecycle.cancel(String.format("Unexpected error \"%s\" on validator creation", new Object[]{var2.getMessage()}));
         throw var2;
      }
   }

   Validator.Future completionFuture() {
      return this.completionFuture;
   }

   Segment segment() {
      return this.lifecycle.segment();
   }

   ValidationMetrics metrics() {
      return this.metrics;
   }

   private TableMetadata table() {
      return this.segment().table;
   }

   private Range<Token> range() {
      return this.segment().range;
   }

   private NodeSyncTracing.SegmentTracing tracing() {
      return this.lifecycle.tracing();
   }

   Validator.Future executeOn(ValidationExecutor executor) {
      if(!this.state.compareAndSet(Validator.State.CREATED, Validator.State.RUNNING)) {
         if(this.state.get() == Validator.State.CANCELLED) {
            return this.completionFuture;
         } else {
            throw new IllegalStateException("Cannot call executeOn multiple times");
         }
      } else {
         ReadCommand command = new NodeSyncReadCommand(this.segment(), NodeSyncHelpers.time().currentTimeSeconds(), executor.asScheduler());
         QueryPager pager = this.createPager(command);
         ReadContext context = ReadContext.builder(command, ConsistencyLevel.TWO).useDigests().blockForAllTargets().observer(this.observer).readRepairDecision(ReadRepairDecision.GLOBAL).readRepairTimeoutInMs(2L * DatabaseDescriptor.getWriteRpcTimeout()).build(System.nanoTime());
         logger.trace("Starting execution of validation on {}", this.segment());

         try {
            this.flowFuture = this.fetchAll(executor, pager, context).lift(Threads.requestOn(executor.asScheduler(), TPCTaskType.NODESYNC_VALIDATION)).flatProcess(FlowablePartitionBase::content).processToFuture().handleAsync((v, t) -> {
               if(t == null) {
                  this.markFinished();
               } else {
                  this.handleError(t, executor);
               }

               return null;
            }, executor.asExecutor());
         } catch (Throwable var6) {
            this.handleError(var6, executor);
         }

         return this.completionFuture;
      }
   }

   @VisibleForTesting
   protected QueryPager createPager(ReadCommand command) {
      return command.getPager((PagingState)null, ProtocolVersion.CURRENT);
   }

   private Flow<FlowablePartition> fetchAll(ValidationExecutor executor, QueryPager pager, ReadContext context) {
      return this.fetchPage(executor, pager, context).concatWith(() -> {
         return this.isDone(pager)?null:this.fetchPage(executor, pager, context.forNewQuery(System.nanoTime()));
      });
   }

   private Flow<FlowablePartition> fetchPage(ValidationExecutor executor, QueryPager pager, ReadContext context) {
      assert !pager.isExhausted();

      this.lifecycle.onNewPage(this.pageSize);
      this.observer.onNewPage();
      executor.onNewPage();
      return Flow.concat(new Flow[]{pager.fetchPage(this.pageSize, context), Flow.defer(() -> {
         this.onPageComplete(executor);
         return Flow.empty();
      })});
   }

   private boolean isDone(QueryPager pager) {
      return pager.isExhausted() || this.state.get() == Validator.State.CANCELLED;
   }

   private void onPageComplete(ValidationExecutor executor) {
      ValidationOutcome outcome = ValidationOutcome.completed(!this.observer.isComplete, this.observer.hasMismatch);
      this.recordPage(outcome, executor);
      this.lifecycle.onCompletedPage(outcome, this.observer.pageMetrics);
   }

   private void handleError(Throwable t, Validator.PageProcessingListener listener) {
      t = Throwables.unwrapped(t);
      if(!(t instanceof CancellationException)) {
         if(t instanceof InvalidatedNodeSyncStateException) {
            this.cancel("Validation invalidated by either a topology change or a depth decrease. The segment will be retried.");
         } else if(isException(t, UnknownKeyspaceException.class, RequestFailureReason.UNKNOWN_KEYSPACE)) {
            this.cancel(String.format("Keyspace %s was dropped while validating", new Object[]{this.table().keyspace}));
         } else if(isException(t, UnknownTableException.class, RequestFailureReason.UNKNOWN_TABLE)) {
            this.cancel(String.format("Table %s was dropped while validating", new Object[]{this.table()}));
         } else if(isException(t, UnknownColumnException.class, RequestFailureReason.UNKNOWN_COLUMN)) {
            this.cancel(String.format("A column in table %s was added or dropped while validating: the segment will be retried.", new Object[]{this.table()}));
         } else if(!(t instanceof UnavailableException) && !(t instanceof RequestTimeoutException)) {
            this.tracing().trace("Unexpected error: {}", (Object)t.getMessage());
            logger.error("Unexpected error during synchronization of table {} on range {}", new Object[]{this.table(), this.range(), t});
            this.recordPage(ValidationOutcome.FAILED, listener);
            this.markFinished();
         } else {
            this.tracing().trace("Unable to complete page (and validation): {}", (Object)t.getMessage());
            this.recordPage(ValidationOutcome.UNCOMPLETED, listener);
            this.markFinished();
         }

      }
   }

   private static boolean isException(Throwable t, Class<?> localExceptionClass, RequestFailureReason remoteFailureReason) {
      if(localExceptionClass.isInstance(t)) {
         return true;
      } else if(!(t instanceof RequestFailureException)) {
         return false;
      } else {
         RequestFailureException rfe = (RequestFailureException)t;
         return rfe.failureReasonByEndpoint.values().stream().anyMatch((r) -> {
            return r == remoteFailureReason;
         });
      }
   }

   boolean cancel(String reason) {
      Validator.State previous = (Validator.State)this.state.getAndUpdate((s) -> {
         return s.isDone()?s:Validator.State.CANCELLED;
      });
      if(previous.isDone()) {
         return previous == Validator.State.CANCELLED;
      } else {
         this.doCancel(reason);
         return true;
      }
   }

   private void doCancel(String reason) {
      try {
         this.lifecycle.cancel(reason);
      } finally {
         if(this.flowFuture != null) {
            this.flowFuture.cancel(true);
         }

         this.completionFuture.actuallyCancel();
      }

   }

   private void markFinished() {
      if(this.lifecycle.isInvalid()) {
         this.cancel("Validation invalidated by either a topology change or a depth decrease. The segment will be retried.");
      } else if(!((Validator.State)this.state.getAndUpdate((s) -> {
         return s.isDone()?s:Validator.State.FINISHED;
      })).isDone()) {
         try {
            this.lifecycle.service().updateMetrics(this.table(), this.metrics);
            ValidationInfo info = new ValidationInfo(this.lifecycle.startTime(), this.validationOutcome, this.missingNodes);
            this.lifecycle.onCompletion(info, this.metrics);
            this.completionFuture.complete(info);
         } catch (Throwable var2) {
            this.doCancel("Failed to mark validation finished due to " + var2.getMessage());
         }
      }
   }

   private void recordPage(ValidationOutcome outcome, Validator.PageProcessingListener listener) {
      this.metrics.addPageOutcome(outcome);
      this.validationOutcome = this.validationOutcome.composeWith(outcome);
      listener.onPageComplete(this.observer.dataValidatedBytesForPage, this.observer.limiterWaitTimeMicrosForPage, this.observer.timeIdleBeforeProcessingPage);
   }

   @VisibleForTesting
   protected class Observer implements ReadReconciliationObserver {
      private volatile boolean isComplete;
      private volatile boolean hasMismatch;
      private volatile long limiterWaitTimeMicrosForPage;
      private volatile long dataValidatedBytesForPage;
      private volatile long responseReceivedTimeNanos;
      private volatile long timeIdleBeforeProcessingPage;
      private volatile ValidationMetrics pageMetrics;

      protected Observer() {
      }

      @VisibleForTesting
      protected void onNewPage() {
         this.hasMismatch = false;
         this.limiterWaitTimeMicrosForPage = 0L;
         this.dataValidatedBytesForPage = 0L;
         this.timeIdleBeforeProcessingPage = 0L;
         if(Validator.this.tracing().isEnabled()) {
            this.pageMetrics = new ValidationMetrics();
         }

      }

      public void queried(Collection<InetAddress> queried) {
         Validator.this.tracing().trace("Querying {} replica: {}", new Object[]{Integer.valueOf(queried.size()), queried});
      }

      public void responsesReceived(Collection<InetAddress> responded) {
         this.responseReceivedTimeNanos = System.nanoTime();
         this.isComplete = responded.size() == Validator.this.replicationFactor;
         if(this.isComplete) {
            Validator.this.tracing().trace("All replica responded ({})", (Object)responded);
         } else {
            if(Validator.this.missingNodes == null) {
               Validator.this.missingNodes = Sets.newConcurrentHashSet();
            }

            Set<InetAddress> missingThisTime = Sets.difference(Validator.this.segmentReplicas, this.setOf(responded));
            Validator.this.missingNodes.addAll(missingThisTime);
            if(Validator.this.tracing().isEnabled()) {
               if(missingThisTime.isEmpty()) {
                  Validator.this.tracing().trace("Partial responses: {} responded ({}) but RF={}", new Object[]{Integer.valueOf(responded.size()), responded, Integer.valueOf(Validator.this.replicationFactor)});
               } else {
                  Validator.this.tracing().trace("Partial responses: {} responded ({}) but {} did not ({})", new Object[]{Integer.valueOf(responded.size()), responded, Integer.valueOf(missingThisTime.size()), missingThisTime});
               }
            }
         }

      }

      private Set<InetAddress> setOf(Collection<InetAddress> c) {
         return (Set)(c instanceof Set?(Set)c:new HashSet(c));
      }

      public void onDigestMatch() {
         this.hasMismatch = false;
      }

      public void onDigestMismatch() {
         this.hasMismatch = true;
         Validator.this.tracing().trace("Digest mismatch, issuing full data request");
      }

      public void onPartition(DecoratedKey partitionKey) {
         this.onData(partitionKey.getKey().remaining(), true);
      }

      public void onPartitionDeletion(DeletionTime deletion, boolean isConsistent) {
         this.onRangeTombstoneMarker((RangeTombstoneMarker)null, isConsistent);
      }

      public void onRow(Row row, boolean isConsistent) {
         Validator.this.metrics.incrementRowsRead(isConsistent);
         if(this.pageMetrics != null) {
            this.pageMetrics.incrementRowsRead(isConsistent);
         }

         this.onData(row.dataSize(), isConsistent);
      }

      public void onRangeTombstoneMarker(RangeTombstoneMarker marker, boolean isConsistent) {
         Validator.this.metrics.incrementRangeTombstoneMarkersRead(isConsistent);
         if(this.pageMetrics != null) {
            this.pageMetrics.incrementRangeTombstoneMarkersRead(isConsistent);
         }

         this.onData(this.dataSize(marker), isConsistent);
      }

      @VisibleForTesting
      protected void onData(int size, boolean isConsistent) {
         assert !TPC.isTPCThread();

         if(this.timeIdleBeforeProcessingPage == 0L) {
            this.timeIdleBeforeProcessingPage = System.nanoTime() - this.responseReceivedTimeNanos;
         }

         Validator.this.metrics.addDataValidated(size, isConsistent);
         if(this.pageMetrics != null) {
            this.pageMetrics.addDataValidated(size, isConsistent);
         }

         this.dataValidatedBytesForPage += (long)size;
         double waitedSeconds = Validator.this.limiter.acquire(size);
         this.limiterWaitTimeMicrosForPage = (long)((double)this.limiterWaitTimeMicrosForPage + waitedSeconds * (double)TimeUnit.SECONDS.toMicros(1L));
      }

      private int dataSize(RangeTombstoneMarker marker) {
         return 12 + (marker == null?0:marker.clustering().dataSize());
      }

      public void onRepair(InetAddress endpoint, PartitionUpdate repair) {
         Validator.this.metrics.addRepair(repair);
         if(this.pageMetrics != null) {
            this.pageMetrics.addRepair(repair);
         }

      }
   }

   class Future extends CompletableFuture<ValidationInfo> {
      private Future() {
      }

      Validator validator() {
         return Validator.this;
      }

      public boolean cancel(boolean mayInterruptIfRunning) {
         return Validator.this.cancel("cancelled through validation future");
      }

      private void actuallyCancel() {
         super.cancel(true);
      }
   }

   interface PageProcessingListener {
      void onNewPage();

      void onPageComplete(long var1, long var3, long var5);
   }

   private static enum State {
      CREATED,
      RUNNING,
      FINISHED,
      CANCELLED;

      private State() {
      }

      boolean isDone() {
         return this == FINISHED || this == CANCELLED;
      }
   }
}
