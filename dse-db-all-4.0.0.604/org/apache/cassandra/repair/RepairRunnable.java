package org.apache.cassandra.repair;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.cassandra.concurrent.JMXConfigurableThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.metrics.Timer;
import org.apache.cassandra.repair.consistent.CoordinatorSession;
import org.apache.cassandra.repair.consistent.SyncStatSummary;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventNotifier;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.apache.cassandra.utils.progress.ProgressListener;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RepairRunnable extends WrappedRunnable implements ProgressEventNotifier {
   private static final Logger logger = LoggerFactory.getLogger(RepairRunnable.class);
   private final StorageService storageService;
   private final int cmd;
   private final RepairOption options;
   private final String keyspace;
   private final String tag;
   private final AtomicInteger progress = new AtomicInteger();
   private final int totalProgress;
   private final List<ProgressListener> listeners = new ArrayList();
   private static final AtomicInteger threadCounter = new AtomicInteger(1);

   public RepairRunnable(StorageService storageService, int cmd, RepairOption options, String keyspace) {
      this.storageService = storageService;
      this.cmd = cmd;
      this.options = options;
      this.keyspace = keyspace;
      this.tag = "repair:" + cmd;
      this.totalProgress = 4 + options.getRanges().size();
   }

   public void addProgressListener(ProgressListener listener) {
      this.listeners.add(listener);
   }

   public void removeProgressListener(ProgressListener listener) {
      this.listeners.remove(listener);
   }

   protected void fireProgressEvent(ProgressEvent event) {
      Iterator var2 = this.listeners.iterator();

      while(var2.hasNext()) {
         ProgressListener listener = (ProgressListener)var2.next();
         listener.progress(this.tag, event);
      }

   }

   protected void fireErrorAndComplete(int progressCount, int totalProgress, String message) {
      StorageMetrics.repairExceptions.inc();
      this.fireProgressEvent(new ProgressEvent(ProgressEventType.ERROR, progressCount, totalProgress, message));
      String completionMessage = String.format("Repair command #%d finished with error", new Object[]{Integer.valueOf(this.cmd)});
      this.fireProgressEvent(new ProgressEvent(ProgressEventType.COMPLETE, progressCount, totalProgress, completionMessage));
      this.recordFailure(message, completionMessage);
   }

   protected void runMayThrow() throws Exception {
      ActiveRepairService.instance.recordRepairStatus(this.cmd, ActiveRepairService.ParentRepairStatus.IN_PROGRESS, ImmutableList.of());
      UUID parentSession = UUIDGen.getTimeUUID();
      AtomicInteger progress = new AtomicInteger();
      int totalProgress = 4 + this.options.getRanges().size();
      String[] specifiedTables = (String[])this.options.getColumnFamilies().toArray(new String[0]);

      Iterable validColumnFamilies;
      try {
         validColumnFamilies = this.storageService.getValidColumnFamilies(false, false, this.keyspace, specifiedTables);
         progress.incrementAndGet();
      } catch (IOException | IllegalArgumentException var34) {
         logger.error("Repair failed:", var34);
         this.fireErrorAndComplete(progress.get(), totalProgress, var34.getMessage());
         return;
      }

      Collection<ColumnFamilyStore> withNodeSync = this.getTablesWithNodeSync(validColumnFamilies);
      if(!withNodeSync.isEmpty()) {
         List<String> tableNames = (List)withNodeSync.stream().map((t) -> {
            return String.format("%s.%s", new Object[]{t.keyspace.getName(), t.name});
         }).sorted().collect(Collectors.toList());
         String message;
         if(specifiedTables.length != 0) {
            message = String.format("Cannot run anti-entropy repair on tables with NodeSync enabled (and NodeSync is enabled on %s)", new Object[]{tableNames});
            this.fireErrorAndComplete(progress.get(), totalProgress, message);
            return;
         }

         message = String.format("Skipping anti-entropy repair on tables with NodeSync enabled: %s.", new Object[]{tableNames});
         this.fireProgressEvent(new ProgressEvent(ProgressEventType.NOTIFICATION, progress.get(), totalProgress, message));
         logger.debug(message);
         validColumnFamilies = Iterables.filter(validColumnFamilies, (t) -> {
            return !withNodeSync.contains(t);
         });
      }

      long startTime = System.currentTimeMillis();
      String message = String.format("Starting repair command #%d (%s), repairing keyspace %s with %s", new Object[]{Integer.valueOf(this.cmd), parentSession, this.keyspace, this.options});
      logger.info(message);
      TraceState traceState;
      if(this.options.isTraced()) {
         StringBuilder cfsb = new StringBuilder();
         Iterator var12 = validColumnFamilies.iterator();

         while(var12.hasNext()) {
            ColumnFamilyStore cfs = (ColumnFamilyStore)var12.next();
            cfsb.append(", ").append(cfs.keyspace.getName()).append(".").append(cfs.name);
         }

         UUID sessionId = Tracing.instance.newSession(Tracing.TraceType.REPAIR);
         traceState = Tracing.instance.begin("repair", ImmutableMap.of("keyspace", this.keyspace, "columnFamilies", cfsb.substring(2)));
         message = message + " tracing with " + sessionId;
         this.fireProgressEvent(new ProgressEvent(ProgressEventType.START, 0, 100, message));
         Tracing.traceRepair(message, new Object[0]);
         traceState.enableActivityNotification(this.tag);
         Iterator var42 = this.listeners.iterator();

         while(var42.hasNext()) {
            ProgressListener listener = (ProgressListener)var42.next();
            traceState.addProgressListener(listener);
         }

         Thread queryThread = this.createQueryThread(this.cmd, sessionId);
         queryThread.setName("RepairTracePolling");
         queryThread.start();
      } else {
         this.fireProgressEvent(new ProgressEvent(ProgressEventType.START, 0, 100, message));
         traceState = null;
      }

      Set<InetAddress> allNeighbors = new HashSet();
      List<RepairRunnable.CommonRange> commonRanges = new ArrayList();
      Collection keyspaceLocalRanges = this.storageService.getLocalRanges(this.keyspace);

      try {
         Iterator var45 = this.options.getRanges().iterator();

         while(var45.hasNext()) {
            Range<Token> range = (Range)var45.next();
            Set<InetAddress> neighbors = ActiveRepairService.getNeighbors(this.keyspace, keyspaceLocalRanges, range, this.options.getDataCenters(), this.options.getHosts());
            if(neighbors != null && !neighbors.isEmpty()) {
               this.addRangeToNeighbors((List)commonRanges, range, neighbors);
               allNeighbors.addAll(neighbors);
            }
         }

         progress.incrementAndGet();
      } catch (IllegalArgumentException var37) {
         logger.error("Repair failed:", var37);
         this.fireErrorAndComplete(progress.get(), totalProgress, var37.getMessage());
         return;
      }

      if(((List)commonRanges).isEmpty()) {
         logger.debug("No common ranges to repair");
         String completionMessage = String.format("Repair command #%d finished with no common ranges to repair", new Object[]{Integer.valueOf(this.cmd)});
         this.fireProgressEvent(new ProgressEvent(ProgressEventType.COMPLETE, progress.get(), totalProgress, completionMessage));
      } else {
         ArrayList columnFamilyStores = new ArrayList();

         try {
            Iterables.addAll(columnFamilyStores, validColumnFamilies);
            progress.incrementAndGet();
         } catch (IllegalArgumentException var33) {
            this.fireErrorAndComplete(progress.get(), totalProgress, var33.getMessage());
            return;
         }

         String[] cfnames = new String[columnFamilyStores.size()];

         for(int i = 0; i < columnFamilyStores.size(); ++i) {
            cfnames[i] = ((ColumnFamilyStore)columnFamilyStores.get(i)).name;
         }

         Set<InetAddress> neighborsToRepair = allNeighbors;
         boolean skippedReplicas = false;
         if(this.options.isForcedRepair()) {
            neighborsToRepair = this.filterLive(allNeighbors);
            commonRanges = filterRanges((List)commonRanges, (Set)neighborsToRepair);
            skippedReplicas = !((Set)neighborsToRepair).equals(allNeighbors);
         }

         if(!this.options.isPreview()) {
            SystemDistributedKeyspace.startParentRepair(parentSession, this.keyspace, cfnames, this.options);
         }

         try {
            Timer.Context ignored = Keyspace.open(this.keyspace).metric.repairPrepareTime.timer();
            Throwable var19 = null;

            try {
               ActiveRepairService.instance.prepareForRepair(parentSession, FBUtilities.getBroadcastAddress(), (Set)neighborsToRepair, this.options, columnFamilyStores, skippedReplicas);
               progress.incrementAndGet();
            } catch (Throwable var32) {
               var19 = var32;
               throw var32;
            } finally {
               if(ignored != null) {
                  if(var19 != null) {
                     try {
                        ignored.close();
                     } catch (Throwable var31) {
                        var19.addSuppressed(var31);
                     }
                  } else {
                     ignored.close();
                  }
               }

            }
         } catch (Throwable var36) {
            if(!this.options.isPreview()) {
               SystemDistributedKeyspace.failParentRepair(parentSession, var36);
            }

            this.fireErrorAndComplete(progress.get(), totalProgress, var36.getMessage());
            return;
         }

         if(this.options.isPreview()) {
            this.previewRepair(parentSession, startTime, (List)commonRanges, cfnames);
         } else if(this.options.isIncremental()) {
            this.incrementalRepair(parentSession, startTime, traceState, (List)commonRanges, skippedReplicas, cfnames);
         } else {
            this.normalRepair(parentSession, startTime, traceState, (List)commonRanges, skippedReplicas, cfnames);
         }

      }
   }

   private Collection<ColumnFamilyStore> getTablesWithNodeSync(Iterable<ColumnFamilyStore> validTables) {
      return Sets.newHashSet(Iterables.filter(validTables, (t) -> {
         return t.metadata().params.nodeSync.isEnabled(t.metadata());
      }));
   }

   private void normalRepair(UUID parentSession, long startTime, TraceState traceState, List<RepairRunnable.CommonRange> commonRanges, boolean skippedReplicas, String... cfnames) {
      ListeningExecutorService executor = this.createExecutor();
      ListenableFuture<List<RepairSessionResult>> allSessions = this.submitRepairSessions(parentSession, false, executor, commonRanges, cfnames);
      final AtomicBoolean hasFailure = new AtomicBoolean();
      ListenableFuture repairResult = Futures.transform(allSessions, new AsyncFunction<List<RepairSessionResult>, Object>() {
         public ListenableFuture apply(List<RepairSessionResult> results) {
            Iterator var2 = results.iterator();

            while(var2.hasNext()) {
               RepairSessionResult sessionResult = (RepairSessionResult)var2.next();
               RepairRunnable.logger.debug("Repair result: {}", results);
               if(sessionResult == null) {
                  hasFailure.compareAndSet(false, true);
               }
            }

            return Futures.immediateFuture((Object)null);
         }
      });
      Futures.addCallback(repairResult, new RepairRunnable.RepairCompleteCallback(parentSession, (Collection)(skippedReplicas?Collections.emptySet():this.flatMapRanges(commonRanges)), startTime, traceState, hasFailure, executor));
   }

   @VisibleForTesting
   static List<RepairRunnable.CommonRange> filterRanges(List<RepairRunnable.CommonRange> commonRanges, Set<InetAddress> participants) {
      List<RepairRunnable.CommonRange> filtered = new ArrayList(commonRanges.size());
      Iterator var3 = commonRanges.iterator();

      while(var3.hasNext()) {
         RepairRunnable.CommonRange commonRange = (RepairRunnable.CommonRange)var3.next();
         Set var10000 = commonRange.endpoints;
         participants.getClass();
         Set<InetAddress> endpoints = ImmutableSet.copyOf(Iterables.filter(var10000, participants::contains));
         if(!endpoints.isEmpty()) {
            filtered.add(new RepairRunnable.CommonRange(endpoints, commonRange.ranges));
         }
      }

      Preconditions.checkState(!filtered.isEmpty(), "Not enough live endpoints for a repair");
      return filtered;
   }

   private void incrementalRepair(UUID parentSession, long startTime, TraceState traceState, List<RepairRunnable.CommonRange> commonRanges, boolean skippedReplicas, String... cfnames) {
      Set<InetAddress> allParticipants = Sets.newHashSet(new InetAddress[]{FBUtilities.getBroadcastAddress()});
      allParticipants.addAll((Collection)commonRanges.stream().flatMap((cr) -> {
         return cr.endpoints.stream();
      }).collect(Collectors.toSet()));
      CoordinatorSession coordinatorSession = ActiveRepairService.instance.consistent.coordinated.registerSession(parentSession, allParticipants);
      ListeningExecutorService executor = this.createExecutor();
      AtomicBoolean hasFailure = new AtomicBoolean(false);
      ListenableFuture repairResult = coordinatorSession.execute(() -> {
         return this.submitRepairSessions(parentSession, true, executor, commonRanges, cfnames);
      }, hasFailure);
      Futures.addCallback(repairResult, new RepairRunnable.RepairCompleteCallback(parentSession, (Collection)(skippedReplicas?Collections.emptySet():this.flatMapRanges(commonRanges)), startTime, traceState, hasFailure, executor));
   }

   private Collection<Range<Token>> flatMapRanges(List<RepairRunnable.CommonRange> allRanges) {
      return (Collection)allRanges.stream().flatMap((cr) -> {
         return cr.ranges.stream();
      }).collect(Collectors.toSet());
   }

   private void previewRepair(final UUID parentSession, final long startTime, List<RepairRunnable.CommonRange> commonRanges, String... cfnames) {
      logger.debug("Starting preview repair for {}", parentSession);
      final ListeningExecutorService executor = this.createExecutor();
      ListenableFuture<List<RepairSessionResult>> allSessions = this.submitRepairSessions(parentSession, false, executor, commonRanges, cfnames);
      Futures.addCallback(allSessions, new FutureCallback<List<RepairSessionResult>>() {
         public void onSuccess(List<RepairSessionResult> results) {
            try {
               PreviewKind previewKind = RepairRunnable.this.options.getPreviewKind();

               assert previewKind != PreviewKind.NONE;

               SyncStatSummary summary = new SyncStatSummary(true);
               summary.consumeSessionResults(results);
               String message;
               if(summary.isEmpty()) {
                  message = previewKind == PreviewKind.REPAIRED?"Repaired data is in sync":"Previewed data was in sync";
                  RepairRunnable.logger.info(message);
                  RepairRunnable.this.fireProgressEvent(new ProgressEvent(ProgressEventType.NOTIFICATION, RepairRunnable.this.progress.get(), RepairRunnable.this.totalProgress, message));
               } else {
                  message = (previewKind == PreviewKind.REPAIRED?"Repaired data is inconsistent\n":"Preview complete\n") + summary.toString();
                  RepairRunnable.logger.info(message);
                  RepairRunnable.this.fireProgressEvent(new ProgressEvent(ProgressEventType.NOTIFICATION, RepairRunnable.this.progress.get(), RepairRunnable.this.totalProgress, message));
               }

               String successMessage = "Repair preview completed successfully";
               RepairRunnable.this.fireProgressEvent(new ProgressEvent(ProgressEventType.SUCCESS, RepairRunnable.this.progress.get(), RepairRunnable.this.totalProgress, successMessage));
               String completionMessage = this.complete();
               ActiveRepairService.instance.recordRepairStatus(RepairRunnable.this.cmd, ActiveRepairService.ParentRepairStatus.COMPLETED, ImmutableList.of(message, successMessage, completionMessage));
            } catch (Throwable var7) {
               RepairRunnable.logger.error("Error completing preview repair", var7);
               this.onFailure(var7);
            }

         }

         public void onFailure(Throwable t) {
            StorageMetrics.repairExceptions.inc();
            RepairRunnable.this.fireProgressEvent(new ProgressEvent(ProgressEventType.ERROR, RepairRunnable.this.progress.get(), RepairRunnable.this.totalProgress, t.getMessage()));
            RepairRunnable.logger.error("Error completing preview repair", t);
            String completionMessage = this.complete();
            RepairRunnable.this.recordFailure(t.getMessage(), completionMessage);
         }

         private String complete() {
            RepairRunnable.logger.debug("Preview repair {} completed", parentSession);
            String duration = DurationFormatUtils.formatDurationWords(System.currentTimeMillis() - startTime, true, true);
            String message = String.format("Repair preview #%d finished in %s", new Object[]{Integer.valueOf(RepairRunnable.this.cmd), duration});
            RepairRunnable.this.fireProgressEvent(new ProgressEvent(ProgressEventType.COMPLETE, RepairRunnable.this.progress.get(), RepairRunnable.this.totalProgress, message));
            executor.shutdownNow();
            return message;
         }
      });
   }

   private ListenableFuture<List<RepairSessionResult>> submitRepairSessions(UUID parentSession, boolean isIncremental, ListeningExecutorService executor, List<RepairRunnable.CommonRange> commonRanges, String... cfnames) {
      List<ListenableFuture<RepairSessionResult>> futures = new ArrayList(this.options.getRanges().size());
      Iterator var7 = commonRanges.iterator();

      while(var7.hasNext()) {
         RepairRunnable.CommonRange cr = (RepairRunnable.CommonRange)var7.next();
         logger.info("Starting RepairSession for {}", cr);
         RepairSession session = ActiveRepairService.instance.submitRepairSession(parentSession, cr.ranges, this.keyspace, this.options.getParallelism(), cr.endpoints, isIncremental, this.options.isPullRepair(), this.options.getPreviewKind(), executor, cfnames);
         if(session != null) {
            Futures.addCallback(session, new RepairRunnable.RepairSessionCallback(session));
            futures.add(session);
         }
      }

      return Futures.successfulAsList(futures);
   }

   private ListeningExecutorService createExecutor() {
      return MoreExecutors.listeningDecorator(new JMXConfigurableThreadPoolExecutor(this.options.getJobThreads(), 2147483647L, TimeUnit.SECONDS, new LinkedBlockingQueue(), new NamedThreadFactory("Repair#" + this.cmd), "internal"));
   }

   private void recordFailure(String failureMessage, String completionMessage) {
      ActiveRepairService.instance.recordRepairStatus(this.cmd, ActiveRepairService.ParentRepairStatus.FAILED, ImmutableList.of(failureMessage, completionMessage));
   }

   private void addRangeToNeighbors(List<RepairRunnable.CommonRange> neighborRangeList, Range<Token> range, Set<InetAddress> neighbors) {
      assert !neighbors.isEmpty();

      for(int i = 0; i < neighborRangeList.size(); ++i) {
         RepairRunnable.CommonRange cr = (RepairRunnable.CommonRange)neighborRangeList.get(i);
         if(cr.endpoints.containsAll(neighbors)) {
            cr.ranges.add(range);
            return;
         }
      }

      List<Range<Token>> ranges = new ArrayList();
      ranges.add(range);
      neighborRangeList.add(new RepairRunnable.CommonRange(neighbors, ranges));
   }

   private Thread createQueryThread(int cmd, final UUID sessionId) {
      return NamedThreadFactory.createThread(new WrappedRunnable() {
         public void runMayThrow() throws Exception {
            TraceState state = Tracing.instance.get(sessionId);
            if(state == null) {
               throw new Exception("no tracestate");
            } else {
               String format = "select event_id, source, activity from %s.%s where session_id = ? and event_id > ? and event_id < ?;";
               String query = String.format(format, new Object[]{"system_traces", "events"});
               SelectStatement statement = (SelectStatement)QueryProcessor.parseStatement(query).prepare().statement;
               ByteBuffer sessionIdBytes = ByteBufferUtil.bytes(sessionId);
               InetAddress source = FBUtilities.getBroadcastAddress();
               HashSet<UUID>[] seen = new HashSet[]{new HashSet(), new HashSet()};
               int si = 0;
               long tlast = System.currentTimeMillis();
               long minWaitMillis = 125L;
               long maxWaitMillis = 1024000L;
               long timeout = minWaitMillis;
               boolean shouldDouble = false;

               TraceState.Status status;
               while((status = state.waitActivity(timeout)) != TraceState.Status.STOPPED) {
                  if(status == TraceState.Status.IDLE) {
                     timeout = shouldDouble?Math.min(timeout * 2L, maxWaitMillis):timeout;
                     shouldDouble = !shouldDouble;
                  } else {
                     timeout = minWaitMillis;
                     shouldDouble = false;
                  }

                  ByteBuffer tminBytes = ByteBufferUtil.bytes(UUIDGen.minTimeUUID(tlast - 1000L));
                  long tcur;
                  ByteBuffer tmaxBytes = ByteBufferUtil.bytes(UUIDGen.maxTimeUUID(tcur = System.currentTimeMillis()));
                  QueryOptions options = QueryOptions.forInternalCalls(ConsistencyLevel.ONE, Lists.newArrayList(new ByteBuffer[]{sessionIdBytes, tminBytes, tmaxBytes}));
                  ResultMessage.Rows rows = (ResultMessage.Rows)statement.execute(QueryState.forInternalCalls(), options, System.nanoTime()).blockingGet();
                  UntypedResultSet result = UntypedResultSet.create(rows.result);
                  Iterator var27 = result.iterator();

                  while(var27.hasNext()) {
                     UntypedResultSet.Row r = (UntypedResultSet.Row)var27.next();
                     if(!source.equals(r.getInetAddress("source"))) {
                        UUID uuid;
                        if((uuid = r.getUUID("event_id")).timestamp() > (tcur - 1000L) * 10000L) {
                           seen[si].add(uuid);
                        }

                        if(!seen[si == 0?1:0].contains(uuid)) {
                           String message = String.format("%s: %s", new Object[]{r.getInetAddress("source"), r.getString("activity")});
                           RepairRunnable.this.fireProgressEvent(new ProgressEvent(ProgressEventType.NOTIFICATION, 0, 0, message));
                        }
                     }
                  }

                  tlast = tcur;
                  si = si == 0?1:0;
                  seen[si].clear();
               }

            }
         }
      }, "Repair-Runnable-" + threadCounter.incrementAndGet());
   }

   private Set<InetAddress> filterLive(Set<InetAddress> allNeighbors) {
      return allNeighbors.stream().filter(FailureDetector.instance::isAlive).collect(Collectors.toSet());
   }

   private class RepairCompleteCallback implements FutureCallback<Object> {
      final UUID parentSession;
      final Collection<Range<Token>> successfulRanges;
      final long startTime;
      final TraceState traceState;
      final AtomicBoolean hasFailure;
      final ExecutorService executor;

      public RepairCompleteCallback(UUID parentSession, Collection<Range<Token>> successfulRanges, long startTime, TraceState traceState, AtomicBoolean hasFailure, ExecutorService executor) {
         this.parentSession = parentSession;
         this.successfulRanges = successfulRanges;
         this.startTime = startTime;
         this.traceState = traceState;
         this.hasFailure = hasFailure;
         this.executor = executor;
      }

      public void onSuccess(Object result) {
         String message;
         if(this.hasFailure.get()) {
            StorageMetrics.repairExceptions.inc();
            message = "Some repair failed";
            RepairRunnable.this.fireProgressEvent(new ProgressEvent(ProgressEventType.ERROR, RepairRunnable.this.progress.get(), RepairRunnable.this.totalProgress, message));
         } else {
            message = "Repair completed successfully";
            RepairRunnable.this.fireProgressEvent(new ProgressEvent(ProgressEventType.SUCCESS, RepairRunnable.this.progress.get(), RepairRunnable.this.totalProgress, message));
         }

         String completionMessage = this.repairComplete();
         if(this.hasFailure.get()) {
            RepairRunnable.this.recordFailure(message, completionMessage);
         } else {
            if(!RepairRunnable.this.options.isPreview()) {
               SystemDistributedKeyspace.successfulParentRepair(this.parentSession, this.successfulRanges);
            }

            ActiveRepairService.instance.recordRepairStatus(RepairRunnable.this.cmd, ActiveRepairService.ParentRepairStatus.COMPLETED, ImmutableList.of(message, completionMessage));
         }

      }

      public void onFailure(Throwable t) {
         StorageMetrics.repairExceptions.inc();
         RepairRunnable.this.fireProgressEvent(new ProgressEvent(ProgressEventType.ERROR, RepairRunnable.this.progress.get(), RepairRunnable.this.totalProgress, t.getMessage()));
         if(!RepairRunnable.this.options.isPreview()) {
            SystemDistributedKeyspace.failParentRepair(this.parentSession, t);
         }

         String completionMessage = this.repairComplete();
         RepairRunnable.this.recordFailure(t.getMessage(), completionMessage);
      }

      private String repairComplete() {
         ActiveRepairService.instance.removeParentRepairSession(this.parentSession);
         long durationMillis = System.currentTimeMillis() - this.startTime;
         String duration = DurationFormatUtils.formatDurationWords(durationMillis, true, true);
         String message = String.format("Repair command #%d finished in %s", new Object[]{Integer.valueOf(RepairRunnable.this.cmd), duration});
         RepairRunnable.this.fireProgressEvent(new ProgressEvent(ProgressEventType.COMPLETE, RepairRunnable.this.progress.get(), RepairRunnable.this.totalProgress, message));
         RepairRunnable.logger.info(message);
         if(RepairRunnable.this.options.isTraced() && this.traceState != null) {
            Iterator var5 = RepairRunnable.this.listeners.iterator();

            while(var5.hasNext()) {
               ProgressListener listener = (ProgressListener)var5.next();
               this.traceState.removeProgressListener(listener);
            }

            Tracing.instance.set(this.traceState);
            Tracing.traceRepair(message, new Object[0]);
            TPCUtils.blockingAwait(Tracing.instance.stopSessionAsync());
         }

         this.executor.shutdown();
         Keyspace.open(RepairRunnable.this.keyspace).metric.repairTime.update(durationMillis, TimeUnit.MILLISECONDS);
         return message;
      }
   }

   private class RepairSessionCallback implements FutureCallback<RepairSessionResult> {
      private final RepairSession session;

      public RepairSessionCallback(RepairSession session) {
         this.session = session;
      }

      public void onSuccess(RepairSessionResult result) {
         String message = String.format("Repair session %s for range %s finished", new Object[]{this.session.getId(), this.session.getRanges().toString()});
         RepairRunnable.logger.info(message);
         RepairRunnable.this.fireProgressEvent(new ProgressEvent(ProgressEventType.PROGRESS, RepairRunnable.this.progress.incrementAndGet(), RepairRunnable.this.totalProgress, message));
      }

      public void onFailure(Throwable t) {
         StorageMetrics.repairExceptions.inc();
         String message = String.format("Repair session %s for range %s failed with error %s", new Object[]{this.session.getId(), this.session.getRanges().toString(), t.getMessage()});
         RepairRunnable.logger.error(message, t);
         RepairRunnable.this.fireProgressEvent(new ProgressEvent(ProgressEventType.ERROR, RepairRunnable.this.progress.incrementAndGet(), RepairRunnable.this.totalProgress, message));
      }
   }

   @VisibleForTesting
   static class CommonRange {
      public final Set<InetAddress> endpoints;
      public final Collection<Range<Token>> ranges;

      public CommonRange(Set<InetAddress> endpoints, Collection<Range<Token>> ranges) {
         Preconditions.checkArgument(endpoints != null && !endpoints.isEmpty());
         Preconditions.checkArgument(ranges != null && !ranges.isEmpty());
         this.endpoints = endpoints;
         this.ranges = ranges;
      }

      public boolean equals(Object o) {
         if(this == o) {
            return true;
         } else if(o != null && this.getClass() == o.getClass()) {
            RepairRunnable.CommonRange that = (RepairRunnable.CommonRange)o;
            return !this.endpoints.equals(that.endpoints)?false:this.ranges.equals(that.ranges);
         } else {
            return false;
         }
      }

      public int hashCode() {
         int result = this.endpoints.hashCode();
         result = 31 * result + this.ranges.hashCode();
         return result;
      }

      public String toString() {
         return "CommonRange{endpoints=" + this.endpoints + ", ranges=" + this.ranges + '}';
      }
   }
}
