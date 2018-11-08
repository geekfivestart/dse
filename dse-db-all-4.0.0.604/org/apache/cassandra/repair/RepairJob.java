package org.apache.cassandra.repair;

import com.google.common.math.IntMath;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.RangeHash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RepairJob extends AbstractFuture<RepairResult> implements Runnable {
   private static Logger logger = LoggerFactory.getLogger(RepairJob.class);
   private final RepairSession session;
   private final RepairJobDesc desc;
   private final RepairParallelism parallelismDegree;
   private final ListeningExecutorService taskExecutor;
   private final boolean isIncremental;
   private final PreviewKind previewKind;
   private int MAX_WAIT_FOR_REMAINING_TASKS_IN_HOURS = 3;

   public RepairJob(RepairSession session, String columnFamily, boolean isIncremental, PreviewKind previewKind) {
      this.session = session;
      this.desc = new RepairJobDesc(session.parentRepairSession, session.getId(), session.keyspace, columnFamily, session.getRanges());
      this.taskExecutor = session.taskExecutor;
      this.parallelismDegree = session.parallelismDegree;
      this.isIncremental = isIncremental;
      this.previewKind = previewKind;
   }

   public void run() {
      Keyspace ks = Keyspace.open(this.desc.keyspace);
      ColumnFamilyStore cfs = ks.getColumnFamilyStore(this.desc.columnFamily);
      cfs.metric.repairsStarted.inc();
      List<InetAddress> allEndpoints = new ArrayList(this.session.endpoints);
      allEndpoints.add(FBUtilities.getBroadcastAddress());
      List validations;
      ArrayList syncTasks;
      List stats;
      if(this.parallelismDegree != RepairParallelism.PARALLEL) {
         syncTasks = new ArrayList(allEndpoints.size());
         ListenableFuture snapshotResult;
         if(this.isIncremental) {
            snapshotResult = Futures.immediateFuture(allEndpoints);
         } else {
            Iterator var7 = allEndpoints.iterator();

            while(var7.hasNext()) {
               InetAddress endpoint = (InetAddress)var7.next();
               SnapshotTask snapshotTask = new SnapshotTask(this.desc, endpoint);
               syncTasks.add(snapshotTask);
               this.taskExecutor.execute(snapshotTask);
            }

            snapshotResult = Futures.allAsList(syncTasks);
         }

         try {
            stats = (List)snapshotResult.get();
            if(this.parallelismDegree == RepairParallelism.SEQUENTIAL) {
               validations = this.sendSequentialValidationRequest(stats);
            } else {
               validations = this.sendDCAwareValidationRequest(stats);
            }
         } catch (Throwable var17) {
            JVMStabilityInspector.inspectThrowable(var17);
            this.waitForRemainingTasksAndFail("snapshot", syncTasks, var17);
            return;
         }
      } else {
         validations = this.sendValidationRequest(allEndpoints);
      }

      Map<InetAddress, Set<RangeHash>> receivedRangeCache = new HashMap();
      syncTasks = new ArrayList();

      try {
         stats = (List)Futures.allAsList(validations).get();
         List<List<TreeResponse>> treesByDc = new ArrayList(((Map)stats.stream().collect(Collectors.groupingBy((t) -> {
            return DatabaseDescriptor.getEndpointSnitch().getDatacenter(t.endpoint);
         }))).values());
         SyncTask previous = null;

         TreeResponse r1;
         TreeResponse r2;
         for(int i = 0; i < treesByDc.size(); ++i) {
            for(int j = i + 1; j < treesByDc.size(); ++j) {
               Iterator var12 = ((List)treesByDc.get(i)).iterator();

               while(var12.hasNext()) {
                  r1 = (TreeResponse)var12.next();

                  for(Iterator var14 = ((List)treesByDc.get(j)).iterator(); var14.hasNext(); previous = this.createSyncTask(receivedRangeCache, syncTasks, previous, r1, r2)) {
                     r2 = (TreeResponse)var14.next();
                  }
               }
            }
         }

         Iterator var23 = treesByDc.iterator();

         while(var23.hasNext()) {
            List<TreeResponse> localDc = (List)var23.next();

            for(int i = 0; i < localDc.size(); ++i) {
               r1 = (TreeResponse)localDc.get(i);

               for(int j = i + 1; j < localDc.size(); ++j) {
                  r2 = (TreeResponse)localDc.get(j);
                  previous = this.createSyncTask(receivedRangeCache, syncTasks, previous, r1, r2);
               }
            }
         }

         assert syncTasks.size() == IntMath.binomial(stats.size(), 2) : "Not enough sync tasks.";

         if(previous != null) {
            this.taskExecutor.submit(previous);
         }
      } catch (Throwable var18) {
         JVMStabilityInspector.inspectThrowable(var18);
         if(this.parallelismDegree == RepairParallelism.SEQUENTIAL) {
            this.failJob(var18);
         } else {
            this.waitForRemainingTasksAndFail("validation", validations, var18);
         }

         return;
      }

      try {
         stats = (List)Futures.allAsList(syncTasks).get();
         if(!this.previewKind.isPreview()) {
            logger.info("[repair #{}] {} is fully synced", this.session.getId(), this.desc.columnFamily);
            SystemDistributedKeyspace.successfulRepairJob(this.session.getId(), this.desc.keyspace, this.desc.columnFamily);
         }

         cfs.metric.repairsCompleted.inc();
         this.set(new RepairResult(this.desc, stats));
      } catch (Throwable var16) {
         cfs.metric.repairsCompleted.inc();
         JVMStabilityInspector.inspectThrowable(var16);
         this.waitForRemainingTasksAndFail("sync", syncTasks, var16);
      }
   }

   private SyncTask createSyncTask(Map<InetAddress, Set<RangeHash>> receivedRangeCache, List<SyncTask> syncTasks, SyncTask previous, TreeResponse r1, TreeResponse r2) {
      InetAddress local = FBUtilities.getLocalAddress();
      Object task;
      if(!r1.endpoint.equals(local) && !r2.endpoint.equals(local)) {
         task = new RemoteSyncTask(this.desc, r1, r2, this.session, this.taskExecutor, previous, receivedRangeCache, this.previewKind);
      } else {
         task = new LocalSyncTask(this.desc, r1, r2, this.isIncremental?this.desc.parentSessionId:null, this.session.pullRepair, this.taskExecutor, previous, receivedRangeCache, this.previewKind);
      }

      syncTasks.add(task);
      return (SyncTask)task;
   }

   private <T> void waitForRemainingTasksAndFail(String phase, Iterable<? extends ListenableFuture<? extends T>> tasks, Throwable t) {
      if(!this.previewKind.isPreview()) {
         logger.warn("[{}] [repair #{}] {} {} failed. Will wait a maximum of {} hours for remaining tasks to finish.", new Object[]{this.session.parentRepairSession, this.session.getId(), this.desc.columnFamily, phase, Integer.valueOf(this.MAX_WAIT_FOR_REMAINING_TASKS_IN_HOURS), t});
      }

      try {
         Futures.successfulAsList(tasks).get((long)this.MAX_WAIT_FOR_REMAINING_TASKS_IN_HOURS, TimeUnit.HOURS);
         if(!this.previewKind.isPreview()) {
            logger.debug("[{}][{}] All remaining repair tasks finished.", this.session.parentRepairSession, this.session.getId());
         }
      } catch (Throwable var5) {
         JVMStabilityInspector.inspectThrowable(var5);
         if(!this.previewKind.isPreview()) {
            logger.warn("[{}] Exception while waiting for remaining repair tasks to complete.", this.session.parentRepairSession, var5);
         }
      }

      this.failJob(t);
   }

   private void failJob(Throwable t) {
      if(!this.previewKind.isPreview()) {
         SystemDistributedKeyspace.failedRepairJob(this.session.getId(), this.desc.keyspace, this.desc.columnFamily, t);
      }

      this.setException(t);
   }

   private List<ListenableFuture<TreeResponse>> sendValidationRequest(Collection<InetAddress> endpoints) {
      String message = String.format("Requesting merkle trees for %s (to %s)", new Object[]{this.desc.columnFamily, endpoints});
      logger.info("[repair #{}] {}", this.desc.sessionId, message);
      Tracing.traceRepair(message, new Object[0]);
      int nowInSec = FBUtilities.nowInSeconds();
      List<ListenableFuture<TreeResponse>> tasks = new ArrayList(endpoints.size());
      Iterator var5 = endpoints.iterator();

      while(var5.hasNext()) {
         InetAddress endpoint = (InetAddress)var5.next();
         ValidationTask task = new ValidationTask(this.desc, endpoint, nowInSec, this.previewKind);
         tasks.add(task);
         this.session.waitForValidation(Pair.create(this.desc, endpoint), task);
         this.taskExecutor.execute(task);
      }

      return tasks;
   }

   private List<ListenableFuture<TreeResponse>> sendSequentialValidationRequest(Collection<InetAddress> endpoints) {
      String message = String.format("Requesting merkle trees for %s (to %s)", new Object[]{this.desc.columnFamily, endpoints});
      logger.info("[repair #{}] {}", this.desc.sessionId, message);
      Tracing.traceRepair(message, new Object[0]);
      int nowInSec = FBUtilities.nowInSeconds();
      List<ListenableFuture<TreeResponse>> tasks = new ArrayList(endpoints.size());
      Queue<InetAddress> requests = new LinkedList(endpoints);
      InetAddress address = (InetAddress)requests.poll();
      ValidationTask firstTask = new ValidationTask(this.desc, address, nowInSec, this.previewKind);
      logger.info("Validating {}", address);
      this.session.waitForValidation(Pair.create(this.desc, address), firstTask);
      tasks.add(firstTask);

      final ValidationTask nextTask;
      for(ValidationTask currentTask = firstTask; requests.size() > 0; currentTask = nextTask) {
         final InetAddress nextAddress = (InetAddress)requests.poll();
         nextTask = new ValidationTask(this.desc, nextAddress, nowInSec, this.previewKind);
         tasks.add(nextTask);
         Futures.addCallback(currentTask, new FutureCallback<TreeResponse>() {
            public void onSuccess(TreeResponse result) {
               RepairJob.logger.info("Validating {}", nextAddress);
               RepairJob.this.session.waitForValidation(Pair.create(RepairJob.this.desc, nextAddress), nextTask);
               RepairJob.this.taskExecutor.execute(nextTask);
            }

            public void onFailure(Throwable t) {
            }
         });
      }

      this.taskExecutor.execute(firstTask);
      return tasks;
   }

   private List<ListenableFuture<TreeResponse>> sendDCAwareValidationRequest(Collection<InetAddress> endpoints) {
      String message = String.format("Requesting merkle trees for %s (to %s)", new Object[]{this.desc.columnFamily, endpoints});
      logger.info("[repair #{}] {}", this.desc.sessionId, message);
      Tracing.traceRepair(message, new Object[0]);
      int nowInSec = FBUtilities.nowInSeconds();
      List<ListenableFuture<TreeResponse>> tasks = new ArrayList(endpoints.size());
      Map<String, Queue<InetAddress>> requestsByDatacenter = new HashMap();

      Iterator var6;
      InetAddress endpoint;
      Object queue;
      for(var6 = endpoints.iterator(); var6.hasNext(); ((Queue)queue).add(endpoint)) {
         endpoint = (InetAddress)var6.next();
         String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(endpoint);
         queue = (Queue)requestsByDatacenter.get(dc);
         if(queue == null) {
            queue = new LinkedList();
            requestsByDatacenter.put(dc, queue);
         }
      }

      var6 = requestsByDatacenter.entrySet().iterator();

      while(var6.hasNext()) {
         Entry<String, Queue<InetAddress>> entry = (Entry)var6.next();
         Queue<InetAddress> requests = (Queue)entry.getValue();
         InetAddress address = (InetAddress)requests.poll();
         ValidationTask firstTask = new ValidationTask(this.desc, address, nowInSec, this.previewKind);
         logger.info("Validating {}", address);
         this.session.waitForValidation(Pair.create(this.desc, address), firstTask);
         tasks.add(firstTask);

         final ValidationTask nextTask;
         for(ValidationTask currentTask = firstTask; requests.size() > 0; currentTask = nextTask) {
            final InetAddress nextAddress = (InetAddress)requests.poll();
            nextTask = new ValidationTask(this.desc, nextAddress, nowInSec, this.previewKind);
            tasks.add(nextTask);
            Futures.addCallback(currentTask, new FutureCallback<TreeResponse>() {
               public void onSuccess(TreeResponse result) {
                  RepairJob.logger.info("Validating {}", nextAddress);
                  RepairJob.this.session.waitForValidation(Pair.create(RepairJob.this.desc, nextAddress), nextTask);
                  RepairJob.this.taskExecutor.execute(nextTask);
               }

               public void onFailure(Throwable t) {
               }
            });
         }

         this.taskExecutor.execute(firstTask);
      }

      return tasks;
   }
}
