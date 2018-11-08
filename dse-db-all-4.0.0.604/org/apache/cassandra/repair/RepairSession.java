package org.apache.cassandra.repair;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.IFailureDetectionEventListener;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.SessionSummary;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTrees;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RepairSession extends AbstractFuture<RepairSessionResult> implements IEndpointStateChangeSubscriber, IFailureDetectionEventListener {
   private static Logger logger = LoggerFactory.getLogger(RepairSession.class);
   public final UUID parentRepairSession;
   private final UUID id;
   public final String keyspace;
   private final String[] cfnames;
   public final RepairParallelism parallelismDegree;
   public final boolean pullRepair;
   public final Collection<Range<Token>> ranges;
   public final Set<InetAddress> endpoints;
   public final boolean isIncremental;
   public final PreviewKind previewKind;
   private final ConcurrentMap<Pair<RepairJobDesc, InetAddress>, ValidationTask> validating = new ConcurrentHashMap();
   private final ConcurrentMap<Pair<RepairJobDesc, NodePair>, RemoteSyncTask> syncingTasks = new ConcurrentHashMap();
   public final ListeningExecutorService taskExecutor = MoreExecutors.listeningDecorator(DebuggableThreadPoolExecutor.createCachedThreadpoolWithMaxSize("RepairJobTask"));
   private volatile boolean terminated = false;

   public RepairSession(UUID parentRepairSession, UUID id, Collection<Range<Token>> ranges, String keyspace, RepairParallelism parallelismDegree, Set<InetAddress> endpoints, boolean isIncremental, boolean pullRepair, PreviewKind previewKind, String... cfnames) {
      assert cfnames.length > 0 : "Repairing no column families seems pointless, doesn't it";

      this.parentRepairSession = parentRepairSession;
      this.id = id;
      this.parallelismDegree = parallelismDegree;
      this.keyspace = keyspace;
      this.cfnames = cfnames;
      this.ranges = ranges;
      this.endpoints = endpoints;
      this.isIncremental = isIncremental;
      this.previewKind = previewKind;
      this.pullRepair = pullRepair;
   }

   public UUID getId() {
      return this.id;
   }

   public Collection<Range<Token>> getRanges() {
      return this.ranges;
   }

   public void waitForValidation(Pair<RepairJobDesc, InetAddress> key, ValidationTask task) {
      this.validating.put(key, task);
   }

   public void waitForSync(Pair<RepairJobDesc, NodePair> key, RemoteSyncTask task) {
      this.syncingTasks.put(key, task);
   }

   public void validationComplete(RepairJobDesc desc, InetAddress endpoint, MerkleTrees trees) {
      ValidationTask task = (ValidationTask)this.validating.remove(Pair.create(desc, endpoint));
      if(task == null) {
         assert this.terminated;

      } else {
         String message = String.format("Received merkle tree for %s from %s", new Object[]{desc.columnFamily, endpoint});
         logger.info("{} {}", this.previewKind.logPrefix(this.getId()), message);
         Tracing.traceRepair(message, new Object[0]);
         task.treesReceived(trees);
      }
   }

   public void syncComplete(RepairJobDesc desc, NodePair nodes, boolean success, List<SessionSummary> summaries) {
      RemoteSyncTask task = (RemoteSyncTask)this.syncingTasks.remove(Pair.create(desc, nodes));
      if(task == null) {
         assert this.terminated;

      } else {
         logger.debug("{} Repair completed between {} and {} on {}", new Object[]{this.previewKind.logPrefix(this.getId()), nodes.endpoint1, nodes.endpoint2, desc.columnFamily});
         task.syncComplete(success, summaries);
      }
   }

   private String repairedNodes() {
      StringBuilder sb = new StringBuilder();
      sb.append(FBUtilities.getBroadcastAddress());
      Iterator var2 = this.endpoints.iterator();

      while(var2.hasNext()) {
         InetAddress ep = (InetAddress)var2.next();
         sb.append(", ").append(ep);
      }

      return sb.toString();
   }

   public void start(ListeningExecutorService executor) {
      if(!this.terminated) {
         logger.info("{} new session: will sync {} on range {} for {}.{}", new Object[]{this.previewKind.logPrefix(this.getId()), this.repairedNodes(), this.ranges, this.keyspace, Arrays.toString(this.cfnames)});
         Tracing.traceRepair("Syncing range {}", new Object[]{this.ranges});
         if(!this.previewKind.isPreview()) {
            SystemDistributedKeyspace.startRepairs(this.getId(), this.parentRepairSession, this.keyspace, this.cfnames, this.ranges, this.endpoints);
         }

         String message;
         if(this.endpoints.isEmpty()) {
            logger.info("{} {}", this.previewKind.logPrefix(this.getId()), message = String.format("No neighbors to repair with on range %s: session completed", new Object[]{this.ranges}));
            Tracing.traceRepair(message, new Object[0]);
            this.set(new RepairSessionResult(this.id, this.keyspace, Lists.newArrayList()));
            if(!this.previewKind.isPreview()) {
               SystemDistributedKeyspace.failRepairs(this.getId(), this.keyspace, this.cfnames, new RuntimeException(message));
            }

         } else {
            Iterator var3 = this.endpoints.iterator();

            while(var3.hasNext()) {
               InetAddress endpoint = (InetAddress)var3.next();
               if(!FailureDetector.instance.isAlive(endpoint)) {
                  message = String.format("Cannot proceed on repair because a neighbor (%s) is dead: session failed", new Object[]{endpoint});
                  logger.error("{} {}", this.previewKind.logPrefix(this.getId()), message);
                  Exception e = new IOException(message);
                  this.setException(e);
                  if(!this.previewKind.isPreview()) {
                     SystemDistributedKeyspace.failRepairs(this.getId(), this.keyspace, this.cfnames, e);
                  }

                  return;
               }
            }

            List<ListenableFuture<RepairResult>> jobs = new ArrayList(this.cfnames.length);
            String[] var10 = this.cfnames;
            int var11 = var10.length;

            for(int var6 = 0; var6 < var11; ++var6) {
               String cfname = var10[var6];
               RepairJob job = new RepairJob(this, cfname, this.isIncremental, this.previewKind);
               executor.submit(job);
               jobs.add(job);
            }

            Futures.addCallback(Futures.allAsList(jobs), new FutureCallback<List<RepairResult>>() {
               public void onSuccess(List<RepairResult> results) {
                  RepairSession.logger.info("{} {}", RepairSession.this.previewKind.logPrefix(RepairSession.this.getId()), "Session completed successfully");
                  Tracing.traceRepair("Completed sync of range {}", new Object[]{RepairSession.this.ranges});
                  RepairSession.this.set(new RepairSessionResult(RepairSession.this.id, RepairSession.this.keyspace, results));
                  RepairSession.this.taskExecutor.shutdown();

                  assert RepairSession.this.validating.isEmpty() && RepairSession.this.syncingTasks.isEmpty() : "All tasks should be finished on RepairJob completion.";

                  RepairSession.this.terminate();
               }

               public void onFailure(Throwable t) {
                  RepairSession.logger.error("{} Session completed with the following error", RepairSession.this.previewKind.logPrefix(RepairSession.this.getId()), t);
                  Tracing.traceRepair("Session completed with the following error: {}", new Object[]{t});
                  RepairSession.this.forceShutdown(t);
               }
            }, this.taskExecutor);
         }
      }
   }

   public void terminate() {
      this.terminated = true;
      Iterator var1 = this.validating.values().iterator();

      while(var1.hasNext()) {
         ValidationTask v = (ValidationTask)var1.next();
         v.treesReceived((MerkleTrees)null);
      }

      this.validating.clear();
      var1 = this.syncingTasks.values().iterator();

      while(var1.hasNext()) {
         RemoteSyncTask s = (RemoteSyncTask)var1.next();
         s.syncComplete(false, (List)null);
      }

      this.syncingTasks.clear();
   }

   public void forceShutdown(Throwable reason) {
      this.setException(reason);
      this.terminate();
      this.taskExecutor.shutdownNow();
   }

   public void onJoin(InetAddress endpoint, EndpointState epState) {
   }

   public void beforeChange(InetAddress endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue) {
   }

   public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value) {
   }

   public void onAlive(InetAddress endpoint, EndpointState state) {
   }

   public void onDead(InetAddress endpoint, EndpointState state) {
   }

   public void onRemove(InetAddress endpoint) {
      this.convict(endpoint, 1.7976931348623157E308D);
   }

   public void onRestart(InetAddress endpoint, EndpointState epState) {
      this.convict(endpoint, 1.7976931348623157E308D);
   }

   public void convict(InetAddress endpoint, double phi) {
      if(this.endpoints.contains(endpoint)) {
         if(phi >= 2.0D * DatabaseDescriptor.getPhiConvictThreshold()) {
            logger.warn("[repair #{}] Endpoint {} died, will fail all the active tasks of this node.", this.getId(), endpoint);
            Iterator it = this.validating.entrySet().iterator();

            Entry entry;
            while(it.hasNext()) {
               entry = (Entry)it.next();
               if(endpoint.equals(((Pair)entry.getKey()).right)) {
                  it.remove();
                  ((ValidationTask)entry.getValue()).treesReceived((MerkleTrees)null);
               }
            }

            it = this.syncingTasks.entrySet().iterator();

            while(it.hasNext()) {
               entry = (Entry)it.next();
               if(endpoint.equals(((NodePair)((Pair)entry.getKey()).right).endpoint1)) {
                  it.remove();
                  ((RemoteSyncTask)entry.getValue()).syncComplete(false, (List)null);
               }
            }

         }
      }
   }
}
