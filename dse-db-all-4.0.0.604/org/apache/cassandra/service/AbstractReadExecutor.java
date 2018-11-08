package org.apache.cassandra.service;

import com.google.common.collect.Iterables;
import io.netty.util.concurrent.FastThreadLocal;
import io.reactivex.Completable;
import io.reactivex.CompletableObserver;
import io.reactivex.CompletableSource;
import io.reactivex.functions.Consumer;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.concurrent.TPCTimeoutTask;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DigestVersion;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadContext;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.locator.ILatencySubscriber;
import org.apache.cassandra.metrics.ReadCoordinationMetrics;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Request;
import org.apache.cassandra.schema.SpeculativeRetryParam;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.flow.Flow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractReadExecutor {
   private static final Logger logger = LoggerFactory.getLogger(AbstractReadExecutor.class);
   protected final ReadCommand command;
   protected final List<InetAddress> targetReplicas;
   protected final ReadCallback<FlowablePartition> handler;
   protected final DigestVersion digestVersion;
   protected final ColumnFamilyStore cfs;
   private static final FastThreadLocal<ArrayList<InetAddress>[]> REUSABLE_REPLICA_LISTS;

   AbstractReadExecutor(ColumnFamilyStore cfs, ReadCommand command, List<InetAddress> targetReplicas, ReadContext ctx) {
      this.cfs = cfs;
      this.command = command;
      this.targetReplicas = targetReplicas;
      this.handler = ReadCallback.forInitialRead(command, targetReplicas, ctx);
      this.digestVersion = DigestVersion.forReplicas(targetReplicas);
   }

   protected Completable makeRequests(List<InetAddress> endpoints, int minDataRead) {
      assert minDataRead > 0 : "Asked for only digest reads, which makes no sense";

      return this.handler.readContext().withDigests && minDataRead < endpoints.size()?this.makeDataRequests(endpoints.subList(0, minDataRead)).concatWith(this.makeDigestRequests(endpoints.subList(minDataRead, endpoints.size()))):this.makeDataRequests(endpoints);
   }

   private Completable makeDataRequests(List<InetAddress> endpoints) {
      assert !endpoints.isEmpty();

      Tracing.trace("Reading data from {}", (Object)endpoints);
      logger.trace("Reading data from {}", endpoints);
      return this.makeRequests(this.command, endpoints);
   }

   private Completable makeDigestRequests(List<InetAddress> endpoints) {
      assert !endpoints.isEmpty();

      Tracing.trace("Reading digests from {}", (Object)endpoints);
      logger.trace("Reading digests from {}", endpoints);
      return this.makeRequests(this.command.createDigestCommand(this.digestVersion), endpoints);
   }

   private Completable makeRequests(ReadCommand readCommand, List<InetAddress> endpoints) {
      MessagingService.instance().send((Request.Dispatcher)readCommand.dispatcherTo(endpoints), this.handler);
      return Completable.complete();
   }

   public abstract Completable maybeTryAdditionalReplicas();

   public abstract List<InetAddress> getContactedReplicas();

   public abstract Completable executeAsync();

   public Flow<FlowablePartition> result() {
      return this.handler.result().doOnError((e) -> {
         if(e instanceof ReadTimeoutException) {
            this.onReadTimeout();
         }

      });
   }

   public static AbstractReadExecutor getReadExecutor(SinglePartitionReadCommand command, ReadContext ctx) throws UnavailableException {
      Keyspace keyspace = ctx.keyspace;
      ConsistencyLevel consistencyLevel = ctx.consistencyLevel;
      ArrayList<InetAddress>[] reusableLists = (ArrayList[])REUSABLE_REPLICA_LISTS.get();
      ArrayList<InetAddress> scratchAllReplicas = reusableLists[0];
      ArrayList scratchTargetReplicas = reusableLists[1];

      AbstractReadExecutor var7;
      try {
         StorageProxy.addLiveSortedEndpointsToList(keyspace, command.partitionKey(), scratchAllReplicas);
         ctx.populateForQuery(scratchAllReplicas, scratchTargetReplicas);
         var7 = getReadExecutor(command, ctx, keyspace, consistencyLevel, scratchAllReplicas, scratchTargetReplicas);
      } finally {
         scratchAllReplicas.clear();
         scratchTargetReplicas.clear();
      }

      return var7;
   }

   private static AbstractReadExecutor getReadExecutor(SinglePartitionReadCommand command, ReadContext ctx, Keyspace keyspace, ConsistencyLevel consistencyLevel, List<InetAddress> scratchAllReplicas, List<InetAddress> scratchTargetReplicas) {
      if(!scratchAllReplicas.contains(FBUtilities.getBroadcastAddress())) {
         ReadCoordinationMetrics.nonreplicaRequests.inc();
      } else if(!scratchTargetReplicas.contains(FBUtilities.getBroadcastAddress())) {
         ReadCoordinationMetrics.preferredOtherReplicas.inc();
      }

      consistencyLevel.assureSufficientLiveNodes(keyspace, scratchTargetReplicas);
      if(ctx.readRepairDecision != ReadRepairDecision.NONE) {
         Tracing.trace("Read-repair {}", (Object)ctx.readRepairDecision);
         ReadRepairMetrics.attempted.mark();
      }

      ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(command.metadata().id);
      SpeculativeRetryParam retry = cfs.metadata().params.speculativeRetry;
      if(retry.equals(SpeculativeRetryParam.NONE) | consistencyLevel == ConsistencyLevel.EACH_QUORUM) {
         return new AbstractReadExecutor.NeverSpeculatingReadExecutor(cfs, command, new ArrayList(scratchTargetReplicas), ctx, false);
      } else if(consistencyLevel.blockFor(keyspace) == scratchAllReplicas.size()) {
         return new AbstractReadExecutor.NeverSpeculatingReadExecutor(cfs, command, new ArrayList(scratchTargetReplicas), ctx, true);
      } else if(scratchTargetReplicas.size() == scratchAllReplicas.size()) {
         return new AbstractReadExecutor.AlwaysSpeculatingReadExecutor(cfs, command, new ArrayList(scratchTargetReplicas), ctx);
      } else {
         InetAddress extraReplica = (InetAddress)scratchAllReplicas.get(scratchTargetReplicas.size());
         if(ctx.readRepairDecision == ReadRepairDecision.DC_LOCAL && scratchTargetReplicas.contains(extraReplica)) {
            for(int i = 0; i < scratchAllReplicas.size(); ++i) {
               InetAddress address = (InetAddress)scratchAllReplicas.get(i);
               if(!scratchTargetReplicas.contains(address)) {
                  extraReplica = address;
                  break;
               }
            }
         }

         scratchTargetReplicas.add(extraReplica);
         return (AbstractReadExecutor)(retry.equals(SpeculativeRetryParam.ALWAYS)?new AbstractReadExecutor.AlwaysSpeculatingReadExecutor(cfs, command, new ArrayList(scratchTargetReplicas), ctx):new AbstractReadExecutor.SpeculatingReadExecutor(cfs, command, new ArrayList(scratchTargetReplicas), ctx));
      }
   }

   boolean shouldSpeculate() {
      return this.cfs.keyspace.getReplicationStrategy().getReplicationFactor() != 1 && this.cfs.sampleLatencyNanos <= TimeUnit.MILLISECONDS.toNanos(this.command.getTimeout());
   }

   void onReadTimeout() {
   }

   static {
      MessagingService.instance().register(ReadCoordinationMetrics::updateReplicaLatency);
      REUSABLE_REPLICA_LISTS = new FastThreadLocal<ArrayList<InetAddress>[]>() {
         protected ArrayList[] initialValue() throws Exception {
            ArrayList[] arrayLists = new ArrayList[]{new ArrayList(), new ArrayList()};
            return arrayLists;
         }
      };
   }

   private static class Disposer implements java.util.function.Consumer<Flow<FlowablePartition>> {
      private final TPCTimeoutTask timeoutTask;

      public Disposer(TPCTimeoutTask timeoutTask) {
         this.timeoutTask = timeoutTask;
      }

      public void accept(Flow<FlowablePartition> ignored) {
         this.timeoutTask.dispose();
      }
   }

   static class AlwaysSpeculatingReadExecutor extends AbstractReadExecutor {
      public AlwaysSpeculatingReadExecutor(ColumnFamilyStore cfs, ReadCommand command, List<InetAddress> targetReplicas, ReadContext ctx) {
         super(cfs, command, targetReplicas, ctx);
      }

      public Completable maybeTryAdditionalReplicas() {
         return Completable.complete();
      }

      public List<InetAddress> getContactedReplicas() {
         return this.targetReplicas;
      }

      public Completable executeAsync() {
         this.cfs.metric.speculativeRetries.inc();
         return this.makeRequests(this.targetReplicas, 2);
      }

      void onReadTimeout() {
         this.cfs.metric.speculativeFailedRetries.inc();
      }
   }

   static class SpeculatingReadExecutor extends AbstractReadExecutor {
      private volatile boolean speculated = false;

      public SpeculatingReadExecutor(ColumnFamilyStore cfs, ReadCommand command, List<InetAddress> targetReplicas, ReadContext ctx) {
         super(cfs, command, targetReplicas, ctx);
      }

      public Completable executeAsync() {
         List<InetAddress> initialReplicas = this.targetReplicas.subList(0, this.targetReplicas.size() - 1);
         return this.handler.blockFor() < initialReplicas.size()?this.makeRequests(initialReplicas, 2):this.makeRequests(initialReplicas, 1);
      }

      public Completable maybeTryAdditionalReplicas() {
         return Completable.defer(() -> {
            if(!this.shouldSpeculate()) {
               return CompletableObserver::onComplete;
            } else {
               TPCTimeoutTask<ReadCallback<FlowablePartition>> timeoutTask = new TPCTimeoutTask(this.handler);
               timeoutTask.submit((callback) -> {
                  if(!callback.hasResult()) {
                     TPC.bestTPCScheduler().execute(() -> {
                        this.speculated = true;
                        this.cfs.metric.speculativeRetries.inc();
                        ReadCommand retryCommand = this.command;
                        if(callback.resolver.isDataPresent() && callback.resolver instanceof DigestResolver) {
                           retryCommand = this.command.createDigestCommand(this.digestVersion);
                        }

                        InetAddress extraReplica = (InetAddress)Iterables.getLast(this.targetReplicas);
                        Tracing.trace("Speculating read retry on {}", (Object)extraReplica);
                        AbstractReadExecutor.logger.trace("Speculating read retry on {}", extraReplica);
                        MessagingService.instance().send((Request)retryCommand.requestTo(extraReplica), callback);
                     }, TPCTaskType.READ_SPECULATE);
                  }

               }, this.cfs.sampleLatencyNanos, TimeUnit.NANOSECONDS);
               this.handler.onResult(new AbstractReadExecutor.Disposer(timeoutTask));
               return CompletableObserver::onComplete;
            }
         });
      }

      public List<InetAddress> getContactedReplicas() {
         return this.speculated?this.targetReplicas:this.targetReplicas.subList(0, this.targetReplicas.size() - 1);
      }

      void onReadTimeout() {
         assert this.speculated;

         this.cfs.metric.speculativeFailedRetries.inc();
      }
   }

   static class NeverSpeculatingReadExecutor extends AbstractReadExecutor {
      private final boolean logFailedSpeculation;

      public NeverSpeculatingReadExecutor(ColumnFamilyStore cfs, ReadCommand command, List<InetAddress> targetReplicas, ReadContext ctx, boolean logFailedSpeculation) {
         super(cfs, command, targetReplicas, ctx);
         this.logFailedSpeculation = logFailedSpeculation;
      }

      public Completable executeAsync() {
         return this.makeRequests(this.targetReplicas, 1);
      }

      public Completable maybeTryAdditionalReplicas() {
         return Completable.defer(() -> {
            if(this.shouldSpeculate() && this.logFailedSpeculation) {
               TPCTimeoutTask<ReadCallback<FlowablePartition>> timeoutTask = new TPCTimeoutTask(this.handler);
               timeoutTask.submit((callback) -> {
                  if(!callback.hasResult()) {
                     this.cfs.metric.speculativeInsufficientReplicas.inc();
                  }

               }, this.cfs.sampleLatencyNanos, TimeUnit.NANOSECONDS);
               this.handler.onResult(new AbstractReadExecutor.Disposer(timeoutTask));
               return CompletableObserver::onComplete;
            } else {
               return CompletableObserver::onComplete;
            }
         });
      }

      public List<InetAddress> getContactedReplicas() {
         return this.targetReplicas;
      }
   }
}
