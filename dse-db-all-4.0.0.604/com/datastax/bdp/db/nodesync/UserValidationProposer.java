package com.datastax.bdp.db.nodesync;

import com.google.common.collect.ImmutableList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.LongStream;
import javax.annotation.Nullable;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.repair.SystemDistributedKeyspace;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.units.RateUnit;
import org.apache.cassandra.utils.units.RateValue;
import org.apache.cassandra.utils.units.TimeValue;

public class UserValidationProposer extends ValidationProposer implements Iterator<ValidationProposal> {
   private final UserValidationID id;
   private final long createdTime = NodeSyncHelpers.time().currentTimeMillis();
   @Nullable
   private final ImmutableList<Range<Token>> validatedRanges;
   @Nullable
   private final Integer rateInKB;
   private final ImmutableList<TableState.Ref> toValidate;
   private volatile int nextIdx;
   private final UserValidationProposer.Future completionFuture = new UserValidationProposer.Future();
   private final AtomicInteger remaining;
   private volatile long startTime = -1L;
   private final AtomicIntegerArray outcomes = new AtomicIntegerArray(ValidationOutcome.values().length);
   private final AtomicReference<ValidationMetrics> metrics = new AtomicReference();

   private UserValidationProposer(UserValidationID id, TableState state, ImmutableList<Range<Token>> validatedRanges, ImmutableList<TableState.Ref> toValidate, Integer rateInKB) {
      super(state);
      this.id = id;
      this.validatedRanges = validatedRanges;
      this.toValidate = toValidate;
      this.remaining = new AtomicInteger(toValidate.size());
      this.rateInKB = rateInKB;
   }

   public UserValidationID id() {
      return this.id;
   }

   Optional<RateValue> rate() {
      return Optional.ofNullable(this.rateInKB == null?null:RateValue.of((long)this.rateInKB.intValue(), RateUnit.KB_S));
   }

   @Nullable
   public List<Range<Token>> validatedRanges() {
      return this.validatedRanges;
   }

   static UserValidationProposer create(NodeSyncState state, UserValidationOptions options) {
      TableMetadata table = options.table;
      ColumnFamilyStore store = Schema.instance.getColumnFamilyStoreInstance(table.id);
      if(store == null) {
         throw new UnknownTableException(String.format("Cannot find table %s, it seems to have been dropped recently", new Object[]{table}), table.id);
      } else {
         int rf = store.keyspace.getReplicationStrategy().getReplicationFactor();
         if(rf <= 1) {
            throw new IllegalArgumentException(String.format("Cannot do validation on table %s as it is not replicated (keyspace %s has replication factor %d)", new Object[]{table, table.keyspace, Integer.valueOf(rf)}));
         } else {
            TableState tableState = state.getOrLoad(table);
            ImmutableList<TableState.Ref> toValidate = tableState.intersectingSegments(options.validatedRanges);
            UserValidationProposer proposer = new UserValidationProposer(options.id, tableState, options.validatedRanges, toValidate, options.rateInKB);
            SystemDistributedKeyspace.recordNodeSyncUserValidation(proposer);
            return proposer;
         }
      }
   }

   UserValidationProposer.Future completionFuture() {
      return this.completionFuture;
   }

   public boolean hasNext() {
      return !this.isDone() && this.nextIdx < this.toValidate.size();
   }

   public ValidationProposal next() {
      return new UserValidationProposer.Proposal((TableState.Ref)this.toValidate.get(this.nextIdx++));
   }

   public boolean isDone() {
      return this.completionFuture.isDone();
   }

   public boolean cancel() {
      boolean cancelled = this.completionFuture.cancel(false);
      SystemDistributedKeyspace.recordNodeSyncUserValidation(this);
      return cancelled;
   }

   public boolean isCancelled() {
      return this.completionFuture.isCancelled();
   }

   private boolean isCompletedExceptionally() {
      return !this.isCancelled() && this.completionFuture.isCompletedExceptionally();
   }

   public UserValidationProposer.Status status() {
      return UserValidationProposer.Status.of(this);
   }

   public UserValidationProposer.Statistics statistics() {
      return this.completionFuture.getCurrent();
   }

   private void onValidationDone(ValidationInfo info, ValidationMetrics metrics) {
      this.outcomes.incrementAndGet(info.outcome.ordinal());
      this.metrics.getAndAccumulate(metrics, (p, n) -> {
         return p == null?n:ValidationMetrics.merge(p, n);
      });
      if(this.remaining.decrementAndGet() == 0) {
         this.completionFuture.complete(new UserValidationProposer.Statistics(this.startTime, NodeSyncHelpers.time().currentTimeMillis(), (ValidationMetrics)this.metrics.get(), this.extractOutcomes(), this.toValidate.size()));
      } else {
         this.completionFuture.signalListeners();
      }

      SystemDistributedKeyspace.recordNodeSyncUserValidation(this);
   }

   private void onValidationError(Throwable error) {
      this.completionFuture.completeExceptionally(error);
      SystemDistributedKeyspace.recordNodeSyncUserValidation(this);
   }

   private long[] extractOutcomes() {
      long[] a = new long[this.outcomes.length()];

      for(int i = 0; i < this.outcomes.length(); ++i) {
         a[i] = (long)this.outcomes.get(i);
      }

      return a;
   }

   public static enum Status {
      RUNNING,
      SUCCESSFUL,
      CANCELLED,
      FAILED;

      private Status() {
      }

      private static UserValidationProposer.Status of(UserValidationProposer proposer) {
         return proposer.isCancelled()?CANCELLED:(proposer.isCompletedExceptionally()?FAILED:(proposer.isDone()?SUCCESSFUL:RUNNING));
      }

      public UserValidationProposer.Status combineWith(UserValidationProposer.Status other) {
         return this != RUNNING && other != RUNNING?(this != FAILED && other != FAILED?(this != CANCELLED && other != CANCELLED?other:CANCELLED):FAILED):RUNNING;
      }

      public static UserValidationProposer.Status from(String s) {
         return valueOf(s.toUpperCase());
      }

      public String toString() {
         return super.toString().toLowerCase();
      }
   }

   public static class Statistics {
      private final long startTime;
      private final long endTime;
      private final long currentTime;
      private final ValidationMetrics metrics;
      private final long[] outcomes;
      private final long segmentsToValidate;

      private Statistics(long startTime, long endTime, ValidationMetrics metrics, long[] outcomes, int segmentsToValidate) {
         this.startTime = startTime;
         this.endTime = endTime;
         this.currentTime = endTime < 0L?NodeSyncHelpers.time().currentTimeMillis():endTime;
         this.metrics = metrics;
         this.outcomes = outcomes;
         this.segmentsToValidate = (long)segmentsToValidate;
      }

      public long startTime() {
         return this.startTime;
      }

      public long endTime() {
         return this.endTime;
      }

      public TimeValue duration() {
         if(this.startTime < 0L) {
            return TimeValue.ZERO;
         } else {
            long durationNanos = Math.max(this.currentTime - this.startTime, 0L);
            return TimeValue.of(durationNanos, TimeUnit.MILLISECONDS);
         }
      }

      public boolean wasFullySuccessful() {
         ValidationOutcome[] var1 = ValidationOutcome.values();
         int var2 = var1.length;

         for(int var3 = 0; var3 < var2; ++var3) {
            ValidationOutcome outcome = var1[var3];
            if(!outcome.wasSuccessful() && this.outcomes[outcome.ordinal()] != 0L) {
               return false;
            }
         }

         return true;
      }

      public long segmentValidated() {
         return LongStream.of(this.outcomes).sum();
      }

      public long segmentsToValidate() {
         return this.segmentsToValidate;
      }

      public long[] getOutcomes() {
         return this.outcomes;
      }

      public long numberOfSegmentsWithOutcome(ValidationOutcome outcome) {
         return this.outcomes[outcome.ordinal()];
      }

      public int progress() {
         int p = (int)((double)this.segmentValidated() / (double)this.segmentsToValidate() * 100.0D);
         return Math.max(0, Math.min(100, p));
      }

      @Nullable
      public ValidationMetrics metrics() {
         return this.metrics;
      }
   }

   private static class Listener {
      private final Consumer<UserValidationProposer.Statistics> consumer;
      private final int frequency;

      private Listener(Consumer<UserValidationProposer.Statistics> consumer, int frequency) {
         this.consumer = consumer;
         this.frequency = frequency;
      }
   }

   public class Future extends CompletableFuture<UserValidationProposer.Statistics> {
      private final List<UserValidationProposer.Listener> listeners;
      private volatile Executor listenerExecutor;

      private Future() {
         this.listeners = new CopyOnWriteArrayList();
      }

      public UserValidationProposer proposer() {
         return UserValidationProposer.this;
      }

      public UserValidationProposer.Statistics getCurrent() {
         try {
            return this.isDone() && !this.isCompletedExceptionally()?(UserValidationProposer.Statistics)this.get():new UserValidationProposer.Statistics(UserValidationProposer.this.startTime, -1L, (ValidationMetrics)UserValidationProposer.this.metrics.get(), UserValidationProposer.this.extractOutcomes(), UserValidationProposer.this.toValidate.size());
         } catch (ExecutionException | InterruptedException var2) {
            throw new AssertionError(var2);
         }
      }

      public void registerProgressListener(Consumer<UserValidationProposer.Statistics> listener, int frequency) {
         if(this.listenerExecutor == null) {
            this.listenerExecutor = Executors.newSingleThreadExecutor(new NamedThreadFactory("UserValidationEventExecutor"));
         }

         if(!this.isDone()) {
            listener.accept(this.getCurrent());
         }

         this.listeners.add(new UserValidationProposer.Listener(listener, frequency));
         this.thenAcceptAsync(listener, this.listenerExecutor);
      }

      private void signalListeners() {
         if(this.listenerExecutor != null) {
            UserValidationProposer.Statistics current = this.getCurrent();
            this.listenerExecutor.execute(() -> {
               this.listeners.forEach((l) -> {
                  if(l.frequency <= 0 || current.progress() % l.frequency == 0) {
                     l.consumer.accept(current);
                  }

               });
            });
         }
      }
   }

   private class Proposal extends ValidationProposal {
      private Proposal(TableState.Ref segmentRef) {
         super(segmentRef);
      }

      Validator activate() {
         if(UserValidationProposer.this.startTime < 0L) {
            UserValidationProposer.this.startTime = NodeSyncHelpers.time().currentTimeMillis();
         }

         NodeSyncTracing.SegmentTracing segTracing = UserValidationProposer.this.service().tracing().forUserValidation(this.segment());
         Validator validator = Validator.create(ValidationLifecycle.createAndStart(this.segmentRef, segTracing));
         validator.completionFuture().thenAccept((i) -> {
            UserValidationProposer.this.onValidationDone(i, validator.metrics());
         }).exceptionally((e) -> {
            UserValidationProposer.this.onValidationError(e);
            return null;
         });
         return validator;
      }

      public String toString() {
         return String.format("%s(user triggered #%s @ %d)", new Object[]{this.segment(), UserValidationProposer.this.id, Long.valueOf(UserValidationProposer.this.createdTime)});
      }
   }
}
