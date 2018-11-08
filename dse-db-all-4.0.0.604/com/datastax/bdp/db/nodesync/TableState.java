package com.datastax.bdp.db.nodesync;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.units.Units;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableState {
   private static final Logger logger = LoggerFactory.getLogger(TableState.class);
   private final NodeSyncService service;
   private volatile TableMetadata table;
   private volatile TableState.StateHolder stateHolder;
   private final TableState.Version version = new TableState.Version();
   private final ReadWriteLock lock = new ReentrantReadWriteLock();

   private TableState(NodeSyncService service, TableMetadata table) {
      this.service = service;
      this.table = table;
      ScheduledExecutors.optionalTasks.scheduleAtFixedRate(this::unlockExpiredLocalLocks, (long)ValidationLifecycle.LOCK_TIMEOUT_SEC, (long)ValidationLifecycle.LOCK_TIMEOUT_SEC, TimeUnit.SECONDS);
   }

   NodeSyncService service() {
      return this.service;
   }

   static TableState load(NodeSyncService service, TableMetadata table, Collection<Range<Token>> localRanges, int depth) {
      TableState tableState = new TableState(service, table);
      tableState.stateHolder = tableState.emptyState(localRanges, depth).populateFromStatusTable();
      return tableState;
   }

   private NodeSyncStatusTableProxy statusTable() {
      return this.service.statusTableProxy;
   }

   private TableState.StateHolder emptyState(Collection<Range<Token>> localRanges, int depth) {
      return new TableState.StateHolder(this.deadline(), Segments.generate(this.table, localRanges, depth));
   }

   private long deadline() {
      return this.table.params.nodeSync.deadlineTarget(this.table, TimeUnit.MILLISECONDS);
   }

   TableMetadata table() {
      return this.table;
   }

   int depth() {
      return this.stateHolder.segments.depth();
   }

   Collection<Range<Token>> localRanges() {
      return this.stateHolder.segments.localRanges();
   }

   private NodeSyncTracing tracing() {
      return this.service.tracing();
   }

   void onTableUpdate(TableMetadata alteredTable) {
      if(alteredTable != this.table) {
         this.lock.writeLock().lock();

         try {
            if(alteredTable != this.table) {
               long newDeadline = this.deadline();
               this.table = alteredTable;
               this.stateHolder.updateTable(this.table);
               if(this.stateHolder.deadlineTargetMs != newDeadline) {
                  logger.debug("Updating NodeSync state and deadline target for {} following table update", this.table);
                  this.tracing().trace("Updating deadline from {} to {} for {}", new Object[]{Units.toString(this.stateHolder.deadlineTargetMs, TimeUnit.MILLISECONDS), Units.toString(newDeadline, TimeUnit.MILLISECONDS), this.table});
                  this.stateHolder.updateDeadline(newDeadline);
               } else {
                  logger.debug("Updating NodeSync state for {} following table update", this.table);
               }

               ++this.version.priority;
               return;
            }
         } finally {
            this.lock.writeLock().unlock();
         }

      }
   }

   void update(int depth) {
      if(depth != this.stateHolder.segments.depth()) {
         this.lock.writeLock().lock();

         try {
            if(depth != this.stateHolder.segments.depth()) {
               logger.debug("Updating NodeSync state for {} to {} as depth have been updated", this.table, Integer.valueOf(depth));
               TableState.StateHolder newStateHolder = this.emptyState(this.stateHolder.segments.localRanges(), depth);
               if(depth < this.stateHolder.segments.depth()) {
                  newStateHolder.populateFromStatusTable();
                  ++this.version.major;
               } else {
                  newStateHolder.updateFrom(this.stateHolder);
                  ++this.version.minor;
               }

               this.tracing().trace("Updating depth from {} to {} for {} (segments count: {} -> {})", new Object[]{Integer.valueOf(this.stateHolder.segments.depth()), Integer.valueOf(depth), this.table, Integer.valueOf(this.stateHolder.size), Integer.valueOf(newStateHolder.size)});
               this.stateHolder = newStateHolder;
               return;
            }
         } finally {
            this.lock.writeLock().unlock();
         }

      }
   }

   void update(Collection<Range<Token>> localRanges) {
      if(!localRanges.equals(this.stateHolder.segments.localRanges())) {
         this.lock.writeLock().lock();

         try {
            if(!localRanges.equals(this.stateHolder.segments.localRanges())) {
               logger.debug("Updating NodeSync state for {} as local ranges have been updated", this.table);
               TableState.StateHolder newStateHolder = this.emptyState(localRanges, this.stateHolder.segments.depth()).populateFromStatusTable();
               this.tracing().trace("Updating local ranges from {} to {} for {} (segments count: {} -> {})", new Object[]{this.stateHolder.segments.localRanges(), localRanges, this.table, Integer.valueOf(this.stateHolder.size), Integer.valueOf(newStateHolder.size)});
               this.stateHolder = newStateHolder;
               ++this.version.major;
               return;
            }
         } finally {
            this.lock.writeLock().unlock();
         }

      }
   }

   TableState.Ref nextSegmentToValidate() {
      this.lock.readLock().lock();

      TableState.Ref var1;
      try {
         var1 = this.stateHolder.nextSegmentToValidate();
      } finally {
         this.lock.readLock().unlock();
      }

      return var1;
   }

   ImmutableList<TableState.Ref> intersectingSegments(List<Range<Token>> localSubRanges) {
      this.lock.readLock().lock();

      ImmutableList var2;
      try {
         var2 = this.stateHolder.intersectingSegments(localSubRanges);
      } finally {
         this.lock.readLock().unlock();
      }

      return var2;
   }

   long oldestSuccessfulValidation() {
      this.lock.readLock().lock();

      long var1;
      try {
         var1 = this.stateHolder.oldestSuccessfulValidation();
      } finally {
         this.lock.readLock().unlock();
      }

      return var1;
   }

   private void unlockExpiredLocalLocks() {
      if(this.stateHolder != null) {
         int nowInSec = NodeSyncHelpers.time().currentTimeSeconds();
         if(this.stateHolder.hasExpiredLocalLocks(nowInSec)) {
            logger.warn("Detected expired (and thus non released) local NodeSync locks. Will force-release them  to fix, but this should normally not happen and if you see this message with any kind of regularity, please report it");
            this.lock.writeLock().lock();

            try {
               this.stateHolder.expireLocalLocks(nowInSec);
            } finally {
               this.lock.writeLock().unlock();
            }
         }

      }
   }

   @VisibleForTesting
   List<SegmentState> dumpSegmentStates() {
      TableState.StateHolder current = this.stateHolder;
      List<SegmentState> segments = new ArrayList(current.size);

      for(int i = 0; i < current.size; ++i) {
         segments.add(current.immutableSegmentState(i));
      }

      return segments;
   }

   private static boolean isRemotelyLocked(NodeSyncRecord record) {
      return record.lockedBy != null && FailureDetector.instance.isAlive(record.lockedBy);
   }

   public String toString() {
      return this.stateHolder.toString();
   }

   private static class Version {
      volatile long major;
      volatile long minor;
      volatile long priority;

      private Version() {
         this(0L, 0L, 0L);
      }

      private Version(long major, long minor, long priority) {
         this.major = major;
         this.minor = minor;
         this.priority = priority;
      }

      TableState.Version copy() {
         return new TableState.Version(this.major, this.minor, this.priority);
      }

      public boolean equals(Object obj) {
         if(!(obj instanceof TableState.Version)) {
            return false;
         } else {
            TableState.Version other = (TableState.Version)obj;
            return this.major == other.major && this.minor == other.minor && this.priority == other.priority;
         }
      }

      public int hashCode() {
         return Objects.hash(new Object[]{Long.valueOf(this.major), Long.valueOf(this.minor), Long.valueOf(this.priority)});
      }

      public String toString() {
         return String.format("[%d,%d,%d]", new Object[]{Long.valueOf(this.major), Long.valueOf(this.minor), Long.valueOf(this.priority)});
      }
   }

   static class Ref {
      private final TableState tableState;
      private final SegmentState segmentAtCreation;
      private final TableState.Version versionAtCreation;
      private final int indexAtCreation;

      private Ref(TableState tableState, int indexAtCreation) {
         this.tableState = tableState;
         this.segmentAtCreation = tableState.stateHolder.immutableSegmentState(indexAtCreation);
         this.versionAtCreation = tableState.version.copy();
         this.indexAtCreation = indexAtCreation;
      }

      NodeSyncService service() {
         return this.tableState.service;
      }

      Segment segment() {
         return this.segmentAtCreation.segment();
      }

      SegmentState segmentStateAtCreation() {
         return this.segmentAtCreation;
      }

      boolean isInvalidated() {
         return this.versionAtCreation.major != this.tableState.version.major;
      }

      SegmentState currentState() {
         this.tableState.lock.readLock().lock();

         SegmentState var1;
         try {
            if(this.isInvalidated()) {
               var1 = null;
               return var1;
            }

            var1 = this.tableState.stateHolder.immutableSegmentState(this.indexAtCreation);
         } finally {
            this.tableState.lock.readLock().unlock();
         }

         return var1;
      }

      void lock() {
         this.doUpdate((rec$, x$0) -> {
            rec$.lockLocally(x$0);
         });
      }

      void refreshLock() {
         this.doUpdate((rec$, x$0) -> {
            rec$.refreshLocalLock(x$0);
         });
      }

      void onCompletedValidation(long validationTime, boolean wasSuccessful) {
         long lastSuccess = wasSuccessful?validationTime:-9223372036854775808L;
         this.doUpdate((s, i) -> {
            s.updateCompletedValidation(i, validationTime, lastSuccess);
         });
      }

      void forceUnlock() {
         this.doUpdate((rec$, x$0) -> {
            rec$.forceLocalUnlock(x$0);
         });
      }

      TableState.Ref.Status checkStatus() {
         if(!this.versionAtCreation.equals(this.tableState.version)) {
            return TableState.Ref.Status.updated(this);
         } else {
            List<NodeSyncRecord> records = this.tableState.statusTable().nodeSyncRecords(this.segment());
            NodeSyncRecord consolidated = NodeSyncRecord.consolidate(this.segment(), records);
            this.tableState.lock.writeLock().lock();

            TableState.Ref.Status var3;
            try {
               if(!this.isInvalidated()) {
                  this.tableState.stateHolder.update(this.indexAtCreation, consolidated);
               }

               if(TableState.isRemotelyLocked(consolidated)) {
                  var3 = TableState.Ref.Status.locked(this, consolidated.lockedBy);
                  return var3;
               }

               var3 = this.versionAtCreation.equals(this.tableState.version)?TableState.Ref.Status.upToDate(this):TableState.Ref.Status.updated(this);
            } finally {
               this.tableState.lock.writeLock().unlock();
            }

            return var3;
         }
      }

      private void doUpdate(TableState.Ref.SegmentUpdater updater) {
         this.tableState.lock.writeLock().lock();

         try {
            if(!this.isInvalidated()) {
               TableState.StateHolder holder = this.tableState.stateHolder;
               if(this.tableState.version.minor == this.versionAtCreation.minor) {
                  updater.update(holder, this.indexAtCreation);
                  return;
               } else {
                  int[] r = holder.segments.findFullyIncludedIn(this.segment());

                  for(int i = r[0]; i < r[1]; ++i) {
                     updater.update(holder, i);
                  }

                  return;
               }
            }
         } finally {
            this.tableState.lock.writeLock().unlock();
         }

      }

      interface SegmentUpdater {
         void update(TableState.StateHolder var1, int var2);
      }

      static class Status {
         private final boolean upToDate;
         private final TableState.Ref ref;
         @Nullable
         private final InetAddress lockedBy;

         private Status(boolean upToDate, TableState.Ref ref, InetAddress lockedBy) {
            this.upToDate = upToDate;
            this.ref = ref;
            this.lockedBy = lockedBy;
         }

         boolean isUpToDate() {
            return this.upToDate;
         }

         boolean isRemotelyLocked() {
            return this.lockedBy != null;
         }

         private static TableState.Ref.Status upToDate(TableState.Ref ref) {
            return new TableState.Ref.Status(true, ref, (InetAddress)null);
         }

         private static TableState.Ref.Status updated(TableState.Ref ref) {
            return new TableState.Ref.Status(false, ref, (InetAddress)null);
         }

         private static TableState.Ref.Status locked(TableState.Ref ref, InetAddress lockedBy) {
            return new TableState.Ref.Status(false, ref, lockedBy);
         }

         public String toString() {
            if(this.upToDate) {
               return "up to date";
            } else if(this.lockedBy != null) {
               return "segment locked by " + this.lockedBy;
            } else {
               SegmentState currentState = this.ref.currentState();
               String reason;
               if(currentState == null) {
                  reason = "the topology or depth has changed";
               } else if(this.ref.segmentAtCreation.lastValidationTimeMs() != currentState.lastValidationTimeMs()) {
                  reason = String.format("was recently validated by another node (%s, previously know: %s)", new Object[]{NodeSyncHelpers.sinceStr(currentState.lastValidationTimeMs()), NodeSyncHelpers.sinceStr(this.ref.segmentAtCreation.lastValidationTimeMs())});
               } else {
                  reason = String.format("segment %s has now higher priority", new Object[]{this.ref.tableState.nextSegmentToValidate().segment()});
               }

               return String.format("state updated: %s", new Object[]{reason});
            }
         }
      }
   }

   private class StateHolder {
      private static final int UNLOCKED = -2147483648;
      private long deadlineTargetMs;
      private Segments segments;
      private final int size;
      private final long[] lastValidations;
      private final long[] lastSuccessfulValidations;
      private final int[] localLocks;
      private final BitSet remoteLocks;
      private int nextIdx;

      private StateHolder(long deadlineTargetMs, Segments segments, long[] lastValidations, long[] lastSuccessfulValidations, int[] localLocks, BitSet remoteLocks) {
         this.deadlineTargetMs = deadlineTargetMs;
         this.segments = segments;
         this.size = segments.size();
         this.lastValidations = lastValidations;
         this.lastSuccessfulValidations = lastSuccessfulValidations;
         this.localLocks = localLocks;
         this.remoteLocks = remoteLocks;

         assert segments.size() == this.size;

         assert lastValidations.length == this.size;

         assert lastSuccessfulValidations.length == this.size;

         assert localLocks.length == this.size;

         assert remoteLocks.size() >= this.size;

      }

      private StateHolder(long deadlineTargetMs, Segments segments) {
         this(deadlineTargetMs, segments, new long[segments.size()], new long[segments.size()], new int[segments.size()], new BitSet(segments.size()));
         Arrays.fill(this.lastValidations, -9223372036854775808L);
         Arrays.fill(this.lastSuccessfulValidations, -9223372036854775808L);
         Arrays.fill(this.localLocks, -2147483648);
      }

      private long priority(int i) {
         return SegmentState.priority(this.lastValidations[i], this.lastSuccessfulValidations[i], this.deadlineTargetMs, this.localLocks[i] != -2147483648, this.remoteLocks.get(i));
      }

      private void setNextIdx(int i) {
         this.nextIdx = i;
         ++TableState.this.version.priority;
      }

      private boolean isLocalLockExpired(int i, int nowInSec) {
         long lock = (long)this.localLocks[i];
         return lock != -2147483648L && lock < (long)nowInSec;
      }

      private boolean hasExpiredLocalLocks(int nowInSec) {
         for(int i = 0; i < this.size; ++i) {
            if(this.isLocalLockExpired(i, nowInSec)) {
               return true;
            }
         }

         return false;
      }

      private void expireLocalLocks(int nowInSec) {
         for(int i = 0; i < this.size; ++i) {
            if(this.isLocalLockExpired(i, nowInSec)) {
               this.localLocks[i] = -2147483648;
            }
         }

         this.updateNextToValidate();
      }

      private void updateNextToValidate() {
         int minIdx = 0;
         long minPriority = this.priority(minIdx);

         for(int i = 1; i < this.size; ++i) {
            long p = this.priority(i);
            if(p < minPriority) {
               minIdx = i;
               minPriority = p;
            }
         }

         if(minIdx != this.nextIdx) {
            this.setNextIdx(minIdx);
         }

      }

      private TableState.StateHolder populateFromStatusTable() {
         this.segments.localRanges().forEach((range) -> {
            if(range.isTrulyWrapAround()) {
               range.unwrap().forEach(this::populateRangeFromStatusTable);
            } else {
               this.populateRangeFromStatusTable(range);
            }

         });
         this.updateNextToValidate();
         return this;
      }

      private void populateRangeFromStatusTable(Range<Token> toLoad) {
         List<NodeSyncRecord> records = TableState.this.statusTable().nodeSyncRecords(TableState.this.table, toLoad);

         for(int i = 0; i < this.size; ++i) {
            this.update(i, NodeSyncRecord.consolidate(this.segments.get(i), records));
         }

      }

      private void updateTable(TableMetadata table) {
         this.segments = Segments.updateTable(this.segments, table);
      }

      private void updateDeadline(long newDeadlineTargetMs) {
         this.deadlineTargetMs = newDeadlineTargetMs;
         this.updateNextToValidate();
      }

      private TableState.Ref newRef(int i) {
         return new TableState.Ref(TableState.this, i);
      }

      private SegmentState immutableSegmentState(int i) {
         return new ImmutableSegmentState(this.segments.get(i), this.lastValidations[i], this.lastSuccessfulValidations[i], this.deadlineTargetMs, this.localLocks[i] != -2147483648, this.remoteLocks.get(i));
      }

      private TableState.Ref nextSegmentToValidate() {
         return this.newRef(this.nextIdx);
      }

      private ImmutableList<TableState.Ref> intersectingSegments(List<Range<Token>> localSubRanges) {
         this.checkAllLocalRanges(localSubRanges);
         Builder<TableState.Ref> builder = ImmutableList.builder();

         for(int i = 0; i < this.size; ++i) {
            Stream var10000 = localSubRanges.stream();
            Range var10001 = this.segments.get(i).range;
            var10001.getClass();
            if(var10000.anyMatch(var10001::intersects)) {
               builder.add(this.newRef(i));
            }
         }

         return builder.build();
      }

      private void checkAllLocalRanges(List<Range<Token>> toCheck) {
         List<Range<Token>> localRanges = this.segments.normalizedLocalRanges();
         Set<Range<Token>> nonLocal = (Set)toCheck.stream().filter((r) -> {
            return localRanges.stream().noneMatch((l) -> {
               return l.contains((AbstractBounds)r);
            });
         }).collect(Collectors.toSet());
         if(!nonLocal.isEmpty()) {
            throw new IllegalArgumentException(String.format("Can only validate local ranges: ranges %s are not (entirely) local to node %s with ranges %s", new Object[]{nonLocal, FBUtilities.getBroadcastAddress(), localRanges}));
         }
      }

      private void updateFrom(TableState.StateHolder o) {
         for(int i = 0; i < o.size; ++i) {
            int[] r = this.segments.findFullyIncludedIn(o.segments.get(i));

            for(int j = r[0]; j < r[1]; ++j) {
               this.updateInternal(j, o.lastValidations[i], o.lastSuccessfulValidations[i], o.localLocks[i], o.remoteLocks.get(i));
            }
         }

         this.updateNextToValidate();
      }

      private void updateInternal(int i, long newLast, long newLastSuccess, int newLocalLock, boolean newRemoteLock) {
         long previousPriority = this.priority(i);
         if(newLast > this.lastValidations[i]) {
            this.lastValidations[i] = newLast;
         }

         if(newLastSuccess > this.lastSuccessfulValidations[i]) {
            this.lastSuccessfulValidations[i] = newLastSuccess;
         }

         this.localLocks[i] = newLocalLock;
         this.remoteLocks.set(i, newRemoteLock);
         long newPriority = this.priority(i);
         if(newPriority != previousPriority) {
            if(i == this.nextIdx) {
               this.updateNextToValidate();
            } else if(newPriority < this.priority(this.nextIdx)) {
               this.setNextIdx(i);
            }
         }

      }

      private void update(int i, NodeSyncRecord record) {
         this.updateInternal(i, record.lastValidationTimeMs(), record.lastSuccessfulValidationTimeMs(), this.localLocks[i], TableState.isRemotelyLocked(record));
      }

      private void lockLocally(int i) {
         this.localLocks[i] = this.newLockExpiration();
         if(this.nextIdx == i) {
            this.updateNextToValidate();
         }

      }

      private void refreshLocalLock(int i) {
         if(this.localLocks[i] != -2147483648) {
            this.localLocks[i] = this.newLockExpiration();
         }

      }

      private void updateCompletedValidation(int i, long last, long lastSuccess) {
         this.updateInternal(i, last, lastSuccess, -2147483648, false);
      }

      private void forceLocalUnlock(int i) {
         this.localLocks[i] = -2147483648;
         if(this.nextIdx != i) {
            this.updateNextToValidate();
         }

      }

      private int newLockExpiration() {
         return NodeSyncHelpers.time().currentTimeSeconds() + ValidationLifecycle.LOCK_TIMEOUT_SEC;
      }

      private long oldestSuccessfulValidation() {
         long min = this.lastSuccessfulValidations[0];

         for(int i = 1; i < this.size; ++i) {
            min = Math.min(min, this.lastSuccessfulValidations[i]);
         }

         return min;
      }

      public String toString() {
         int LINES = 6;
         List<List<String>> lines = new ArrayList(LINES);

         int i;
         for(i = 0; i < LINES; ++i) {
            lines.add(new ArrayList(this.size));
         }

         for(i = 0; i < this.size; ++i) {
            ((List)lines.get(0)).add(this.segments.get(i).range.toString());
            ((List)lines.get(1)).add(Long.toString(this.lastValidations[i]));
            ((List)lines.get(2)).add(Long.toString(this.lastSuccessfulValidations[i]));
            ((List)lines.get(3)).add(Integer.toString(this.localLocks[i]));
            ((List)lines.get(4)).add(Boolean.toString(this.remoteLocks.get(i)));
            ((List)lines.get(5)).add(Long.toString(this.priority(i)));
         }

         int[] widths = new int[this.size];

         int ixx;
         for(int ix = 0; ix < this.size; ++ix) {
            for(ixx = 0; ixx < LINES; ++ixx) {
               widths[ix] = Math.max(widths[ix], ((String)((List)lines.get(ixx)).get(ix)).length());
            }
         }

         StringBuilder sb = new StringBuilder();
         sb.append("Version = ").append(TableState.this.version).append(", next to validate = ").append(this.nextIdx).append('\n');
         sb.append("        ");

         for(ixx = 0; ixx < this.size; ++ixx) {
            sb.append(" | ").append(this.pad((String)((List)lines.get(0)).get(ixx), widths[ixx]));
         }

         sb.append("\nlast    ");

         for(ixx = 0; ixx < this.size; ++ixx) {
            sb.append(" | ").append(this.pad((String)((List)lines.get(1)).get(ixx), widths[ixx]));
         }

         sb.append("\nlastSucc");

         for(ixx = 0; ixx < this.size; ++ixx) {
            sb.append(" | ").append(this.pad((String)((List)lines.get(2)).get(ixx), widths[ixx]));
         }

         sb.append("\nL. locks");

         for(ixx = 0; ixx < this.size; ++ixx) {
            sb.append(" | ").append(this.pad((String)((List)lines.get(3)).get(ixx), widths[ixx]));
         }

         sb.append("\nR. lock ");

         for(ixx = 0; ixx < this.size; ++ixx) {
            sb.append(" | ").append(this.pad((String)((List)lines.get(4)).get(ixx), widths[ixx]));
         }

         sb.append("\npriority");

         for(ixx = 0; ixx < this.size; ++ixx) {
            sb.append(" | ").append(this.pad((String)((List)lines.get(5)).get(ixx), widths[ixx]));
         }

         return sb.toString();
      }

      private String pad(String val, int width) {
         int spaces = width - val.length();
         StringBuilder sb = new StringBuilder();

         for(int i = 0; i < spaces; ++i) {
            sb.append(' ');
         }

         sb.append(val);
         return sb.toString();
      }
   }
}
