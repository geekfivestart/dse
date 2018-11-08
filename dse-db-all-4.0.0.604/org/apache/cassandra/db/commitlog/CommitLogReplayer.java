package org.apache.cassandra.db.commitlog;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import io.reactivex.Completable;
import io.reactivex.Single;
import io.reactivex.functions.Action;
import io.reactivex.functions.Consumer;
import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.commons.lang3.StringUtils;
import org.jctools.maps.NonBlockingHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitLogReplayer implements CommitLogReadHandler {
   @VisibleForTesting
   public static long MAX_OUTSTANDING_REPLAY_BYTES = Long.getLong("cassandra.commitlog_max_outstanding_replay_bytes", 67108864L).longValue();
   @VisibleForTesting
   public static CommitLogReplayer.MutationInitiator mutationInitiator = new CommitLogReplayer.MutationInitiator();
   static final String IGNORE_REPLAY_ERRORS_PROPERTY = "cassandra.commitlog.ignorereplayerrors";
   private static final Logger logger = LoggerFactory.getLogger(CommitLogReplayer.class);
   private static final int MAX_OUTSTANDING_REPLAY_COUNT = Integer.getInteger("cassandra.commitlog_max_outstanding_replay_count", 1024).intValue();
   private final Set<Keyspace> keyspacesReplayed = new NonBlockingHashSet();
   private final Queue<Future<Integer>> futures = new ArrayDeque();
   private final AtomicInteger replayedCount = new AtomicInteger();
   private final Map<TableId, IntervalSet<CommitLogPosition>> cfPersisted;
   private final CommitLogPosition globalPosition;
   private long pendingMutationBytes = 0L;
   private final CommitLogReplayer.ReplayFilter replayFilter;
   private final CommitLogArchiver archiver;
   @VisibleForTesting
   protected CommitLogReader commitLogReader;
   public final OpOrder writeOrder;

   CommitLogReplayer(CommitLog commitLog, CommitLogPosition globalPosition, Map<TableId, IntervalSet<CommitLogPosition>> cfPersisted, CommitLogReplayer.ReplayFilter replayFilter) {
      this.cfPersisted = cfPersisted;
      this.globalPosition = globalPosition;
      this.replayFilter = replayFilter;
      this.archiver = commitLog.archiver;
      this.commitLogReader = new CommitLogReader();
      this.writeOrder = TPC.newOpOrder(CommitLogReplayer.class);
   }

   public static CommitLogReplayer construct(CommitLog commitLog) {
      Map<TableId, Pair<CommitLogPosition, Long>> truncationRecords = (Map)TPCUtils.blockingGet(SystemKeyspace.readTruncationRecords());
      Map<TableId, IntervalSet<CommitLogPosition>> cfPersisted = new HashMap();
      CommitLogReplayer.ReplayFilter replayFilter = CommitLogReplayer.ReplayFilter.create();
      Iterator var4 = ColumnFamilyStore.all().iterator();

      while(var4.hasNext()) {
         ColumnFamilyStore cfs = (ColumnFamilyStore)var4.next();
         Pair<CommitLogPosition, Long> truncationRecord = (Pair)truncationRecords.get(cfs.metadata.id);
         CommitLogPosition truncatedAt = truncationRecord == null?null:(CommitLogPosition)truncationRecord.left;
         if(truncatedAt != null) {
            long restoreTime = commitLog.archiver.restorePointInTime;
            long truncatedTime = ((Long)truncationRecord.right).longValue();
            if(truncatedTime > restoreTime && replayFilter.includes(cfs.metadata)) {
               logger.info("Restore point in time is before latest truncation of table {}.{}. Clearing truncation record.", cfs.metadata.keyspace, cfs.metadata.name);
               TPCUtils.blockingAwait(SystemKeyspace.removeTruncationRecord(cfs.metadata.id));
               truncatedAt = null;
            }
         }

         IntervalSet<CommitLogPosition> filter = persistedIntervals(cfs.getLiveSSTables(), truncatedAt);
         cfPersisted.put(cfs.metadata.id, filter);
      }

      CommitLogPosition globalPosition = firstNotCovered(cfPersisted.values());
      logger.debug("Global replay position is {} from columnfamilies {}", globalPosition, FBUtilities.toString((Map)cfPersisted));
      return new CommitLogReplayer(commitLog, globalPosition, cfPersisted, replayFilter);
   }

   public void replayPath(File file, boolean tolerateTruncation) throws IOException {
      this.commitLogReader.readCommitLogSegment(this, file, this.globalPosition, -1, tolerateTruncation);
   }

   public void replayFiles(File[] clogs) throws IOException {
      this.commitLogReader.readAllFiles(this, clogs, this.globalPosition);
   }

   public int blockForWrites() {
      Iterator var1 = this.commitLogReader.getInvalidMutations().iterator();

      while(var1.hasNext()) {
         Entry<TableId, AtomicInteger> entry = (Entry)var1.next();
         logger.warn("Skipped {} mutations from unknown (probably removed) CF with id {}", entry.getValue(), entry.getKey());
      }

      FBUtilities.waitOnFutures(this.futures);
      logger.trace("Finished waiting on mutations from recovery");
      this.futures.clear();
      boolean flushingSystem = false;
      List<CompletableFuture<CommitLogPosition>> futures = new ArrayList();

      Keyspace keyspace;
      for(Iterator var3 = this.keyspacesReplayed.iterator(); var3.hasNext(); futures.addAll(keyspace.flush())) {
         keyspace = (Keyspace)var3.next();
         if(keyspace.getName().equals("system")) {
            flushingSystem = true;
         }
      }

      if(!flushingSystem) {
         futures.add(Keyspace.open("system").getColumnFamilyStore("batches").forceFlush());
      }

      FBUtilities.waitOnFutures(futures);
      return this.replayedCount.get();
   }

   public static IntervalSet<CommitLogPosition> persistedIntervals(Iterable<SSTableReader> onDisk, CommitLogPosition truncatedAt) {
      IntervalSet.Builder<CommitLogPosition> builder = new IntervalSet.Builder();
      Iterator var3 = onDisk.iterator();

      while(var3.hasNext()) {
         SSTableReader reader = (SSTableReader)var3.next();
         builder.addAll(reader.getSSTableMetadata().commitLogIntervals);
      }

      if(truncatedAt != null) {
         builder.add(CommitLogPosition.NONE, truncatedAt);
      }

      return builder.build();
   }

   public static CommitLogPosition firstNotCovered(Collection<IntervalSet<CommitLogPosition>> ranges) {
      return (CommitLogPosition)ranges.stream().map((intervals) -> {
         return (CommitLogPosition)Iterables.getFirst(intervals.ends(), CommitLogPosition.NONE);
      }).min(Ordering.natural()).get();
   }

   private boolean shouldReplay(TableId tableId, CommitLogPosition position) {
      return this.cfPersisted.get(tableId) == null?true:!((IntervalSet)this.cfPersisted.get(tableId)).contains(position);
   }

   protected boolean pointInTimeExceeded(Mutation fm) {
      long restoreTarget = this.archiver.restorePointInTime;
      Iterator var4 = fm.getPartitionUpdates().iterator();

      PartitionUpdate upd;
      do {
         if(!var4.hasNext()) {
            return false;
         }

         upd = (PartitionUpdate)var4.next();
      } while(this.archiver.precision.toMillis(upd.maxTimestamp()) <= restoreTarget);

      return true;
   }

   public void handleMutation(Mutation m, int size, int entryLocation, CommitLogDescriptor desc) {
      this.pendingMutationBytes += (long)size;
      boolean isSchemaMutation = SchemaConstants.isSchemaKeyspace(m.getKeyspaceName());
      if(isSchemaMutation) {
         this.writeOrder.awaitNewBarrier();
      }

      this.futures.offer(mutationInitiator.initiateMutation(m, desc.id, size, entryLocation, this));

      while(this.futures.size() > MAX_OUTSTANDING_REPLAY_COUNT || this.pendingMutationBytes > MAX_OUTSTANDING_REPLAY_BYTES || !this.futures.isEmpty() && (((Future)this.futures.peek()).isDone() || isSchemaMutation)) {
         this.pendingMutationBytes -= (long)((Integer)FBUtilities.waitOnFuture((Future)this.futures.poll())).intValue();
      }

   }

   public boolean shouldSkipSegmentOnError(CommitLogReadHandler.CommitLogReadException exception) {
      if(exception.permissible) {
         logger.error("Ignoring commit log replay error likely due to incomplete flush to disk", exception);
      } else {
         if(!Boolean.getBoolean("cassandra.commitlog.ignorereplayerrors")) {
            logger.error("Replay stopped. If you wish to override this error and continue starting the node ignoring commit log replay problems, specify -Dcassandra.commitlog.ignorereplayerrors=true on the command line");
            Throwable t = new CommitLogReplayer.CommitLogReplayException(exception.getMessage(), exception);
            JVMStabilityInspector.killJVM(t, false);
            throw new RuntimeException("JVM killed");
         }

         logger.error("Ignoring commit log replay error", exception);
      }

      return false;
   }

   public void handleUnrecoverableError(CommitLogReadHandler.CommitLogReadException exception) {
      this.shouldSkipSegmentOnError(exception);
   }

   public static class CommitLogReplayException extends IOException {
      public CommitLogReplayException(String message, Throwable cause) {
         super(message, cause);
      }

      public CommitLogReplayException(String message) {
         super(message);
      }
   }

   private static class CustomReplayFilter extends CommitLogReplayer.ReplayFilter {
      private Multimap<String, String> toReplay;

      public CustomReplayFilter(Multimap<String, String> toReplay) {
         this.toReplay = toReplay;
      }

      public Iterable<PartitionUpdate> filter(Mutation mutation) {
         final Collection<String> cfNames = this.toReplay.get(mutation.getKeyspaceName());
         return (Iterable)(cfNames == null?Collections.emptySet():Iterables.filter(mutation.getPartitionUpdates(), new Predicate<PartitionUpdate>() {
            public boolean apply(PartitionUpdate upd) {
               return cfNames.contains(upd.metadata().name);
            }
         }));
      }

      public boolean includes(TableMetadataRef metadata) {
         return this.toReplay.containsEntry(metadata.keyspace, metadata.name);
      }
   }

   private static class AlwaysReplayFilter extends CommitLogReplayer.ReplayFilter {
      private AlwaysReplayFilter() {
      }

      public Iterable<PartitionUpdate> filter(Mutation mutation) {
         return mutation.getPartitionUpdates();
      }

      public boolean includes(TableMetadataRef metadata) {
         return true;
      }
   }

   abstract static class ReplayFilter {
      ReplayFilter() {
      }

      public abstract Iterable<PartitionUpdate> filter(Mutation var1);

      public abstract boolean includes(TableMetadataRef var1);

      public static CommitLogReplayer.ReplayFilter create() {
         if(System.getProperty("cassandra.replayList") == null) {
            return new CommitLogReplayer.AlwaysReplayFilter();
         } else {
            Multimap<String, String> toReplay = HashMultimap.create();
            String[] var1 = System.getProperty("cassandra.replayList").split(",");
            int var2 = var1.length;

            for(int var3 = 0; var3 < var2; ++var3) {
               String rawPair = var1[var3];
               String[] pair = StringUtils.split(rawPair.trim(), '.');
               if(pair.length != 2) {
                  throw new IllegalArgumentException("Each table to be replayed must be fully qualified with keyspace name, e.g., 'system.peers'");
               }

               Keyspace ks = Schema.instance.getKeyspaceInstance(pair[0]);
               if(ks == null) {
                  throw new IllegalArgumentException("Unknown keyspace " + pair[0]);
               }

               ColumnFamilyStore cfs = ks.getColumnFamilyStore(pair[1]);
               if(cfs == null) {
                  throw new IllegalArgumentException(String.format("Unknown table %s.%s", new Object[]{pair[0], pair[1]}));
               }

               toReplay.put(pair[0], pair[1]);
            }

            return new CommitLogReplayer.CustomReplayFilter(toReplay);
         }
      }
   }

   @VisibleForTesting
   public static class MutationInitiator {
      public MutationInitiator() {
      }

      protected Future<Integer> initiateMutation(Mutation mutation, long segmentId, int serializedSize, int entryLocation, CommitLogReplayer commitLogReplayer) {
         Completable completable = Completable.defer(() -> {
            if(Schema.instance.getKeyspaceMetadata(mutation.getKeyspaceName()) == null) {
               return Completable.complete();
            } else if(commitLogReplayer.pointInTimeExceeded(mutation)) {
               return Completable.complete();
            } else {
               Keyspace keyspace = Keyspace.open(mutation.getKeyspaceName());
               Mutation newMutation = null;
               Iterator var7 = commitLogReplayer.replayFilter.filter(mutation).iterator();

               while(var7.hasNext()) {
                  PartitionUpdate update = (PartitionUpdate)var7.next();
                  if(Schema.instance.getTableMetadata(update.metadata().id) != null && commitLogReplayer.shouldReplay(update.metadata().id, new CommitLogPosition(segmentId, entryLocation))) {
                     if(newMutation == null) {
                        newMutation = new Mutation(mutation.getKeyspaceName(), mutation.key());
                     }

                     newMutation.add(update);
                     if(CommitLogReplayer.logger.isTraceEnabled()) {
                        CommitLogReplayer.logger.trace("Replaying {}", update);
                     }

                     commitLogReplayer.replayedCount.incrementAndGet();
                  }
               }

               if(newMutation != null) {
                  assert !newMutation.isEmpty();

                  Completable mutationCompletable = Keyspace.open(newMutation.getKeyspaceName()).apply(newMutation, false, true, false);
                  commitLogReplayer.keyspacesReplayed.add(keyspace);
                  if(SchemaConstants.isSchemaKeyspace(mutation.getKeyspaceName())) {
                     return mutationCompletable.doOnComplete(() -> {
                        Schema.instance.reloadSchema();
                     });
                  } else {
                     return mutationCompletable;
                  }
               } else {
                  return Completable.complete();
               }
            }
         });
         OpOrder var10000 = commitLogReplayer.writeOrder;
         commitLogReplayer.writeOrder.getClass();
         return TPCUtils.toFuture(Single.using(var10000::start, (opOrder) -> {
            return completable.toSingleDefault(Integer.valueOf(serializedSize));
         }, (opOrder) -> {
            opOrder.close();
         }));
      }
   }
}
