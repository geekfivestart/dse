package org.apache.cassandra.db.compaction;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.RateLimiter;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.LongPredicate;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.lifecycle.SSTableIntervalTree;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileAccessType;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.utils.OverlapIterator;
import org.apache.cassandra.utils.concurrent.Refs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactionController implements AutoCloseable {
   private static final Logger logger = LoggerFactory.getLogger(CompactionController.class);
   static final boolean NEVER_PURGE_TOMBSTONES = Boolean.getBoolean("cassandra.never_purge_tombstones");
   public final ColumnFamilyStore cfs;
   private final boolean compactingRepaired;
   private Refs<SSTableReader> overlappingSSTables;
   private OverlapIterator<PartitionPosition, SSTableReader> overlapIterator;
   private final Iterable<SSTableReader> compacting;
   private final boolean ignoreOverlaps;
   private final RateLimiter limiter;
   private final long minTimestamp;
   final CompactionParams.TombstoneOption tombstoneOption;
   final Map<SSTableReader, FileDataInput> openDataFiles;
   public final int gcBefore;

   protected CompactionController(ColumnFamilyStore cfs, int maxValue) {
      this(cfs, (Set)null, maxValue);
   }

   public CompactionController(ColumnFamilyStore cfs, Set<SSTableReader> compacting, int gcBefore) {
      this(cfs, compacting, gcBefore, false);
   }

   public CompactionController(ColumnFamilyStore cfs, Set<SSTableReader> compacting, int gcBefore, boolean ignoreOverlaps) {
      this(cfs, compacting, gcBefore, (RateLimiter)null, cfs.getCompactionStrategyManager().getCompactionParams().tombstoneOption(), ignoreOverlaps);
   }

   public CompactionController(ColumnFamilyStore cfs, Set<SSTableReader> compacting, int gcBefore, RateLimiter limiter, CompactionParams.TombstoneOption tombstoneOption) {
      this(cfs, compacting, gcBefore, limiter, tombstoneOption, false);
   }

   public CompactionController(ColumnFamilyStore cfs, Set<SSTableReader> compacting, int gcBefore, RateLimiter limiter, CompactionParams.TombstoneOption tombstoneOption, boolean ignoreOverlaps) {
      this.openDataFiles = new HashMap();

      assert cfs != null;

      this.cfs = cfs;
      this.gcBefore = gcBefore;
      this.compacting = compacting;
      this.limiter = limiter;
      this.compactingRepaired = compacting != null && compacting.stream().allMatch(SSTableReader::isRepaired);
      this.tombstoneOption = tombstoneOption;
      this.minTimestamp = compacting != null && !compacting.isEmpty()?compacting.stream().mapToLong(SSTableReader::getMinTimestamp).min().getAsLong():0L;
      this.ignoreOverlaps = ignoreOverlaps;
      this.refreshOverlaps();
      if(NEVER_PURGE_TOMBSTONES) {
         logger.warn("You are running with -Dcassandra.never_purge_tombstones=true, this is dangerous!");
      }

   }

   public void maybeRefreshOverlaps() {
      if(NEVER_PURGE_TOMBSTONES) {
         logger.trace("not refreshing overlaps - running with -Dcassandra.never_purge_tombstones=true");
      } else if(this.ignoreOverlaps()) {
         logger.trace("not refreshing overlaps - running with ignoreOverlaps activated");
      } else {
         Iterator var1 = this.overlappingSSTables.iterator();

         SSTableReader reader;
         do {
            if(!var1.hasNext()) {
               return;
            }

            reader = (SSTableReader)var1.next();
         } while(!reader.isMarkedCompacted());

         this.refreshOverlaps();
      }
   }

   private void refreshOverlaps() {
      if(!NEVER_PURGE_TOMBSTONES && !this.ignoreOverlaps()) {
         if(this.overlappingSSTables != null) {
            this.close();
         }

         if(this.compacting == null) {
            this.overlappingSSTables = Refs.tryRef((Iterable)Collections.emptyList());
         } else {
            this.overlappingSSTables = this.cfs.getAndReferenceOverlappingLiveSSTables(this.compacting);
         }

         this.overlapIterator = new OverlapIterator(SSTableIntervalTree.buildIntervals(this.overlappingSSTables));
      }
   }

   public Set<SSTableReader> getFullyExpiredSSTables() {
      return getFullyExpiredSSTables(this.cfs, this.compacting, this.overlappingSSTables, this.gcBefore, this.ignoreOverlaps());
   }

   public static Set<SSTableReader> getFullyExpiredSSTables(ColumnFamilyStore cfStore, Iterable<SSTableReader> compacting, Iterable<SSTableReader> overlapping, int gcBefore) {
      return getFullyExpiredSSTables(cfStore, compacting, overlapping, gcBefore, false);
   }

   public static Set<SSTableReader> getFullyExpiredSSTables(ColumnFamilyStore cfStore, Iterable<SSTableReader> compacting, Iterable<SSTableReader> overlappingByKey, int gcBefore, boolean ignoreOverlaps) {
      logger.trace("Checking droppable sstables in {}", cfStore);
      if(!NEVER_PURGE_TOMBSTONES && compacting != null) {
         if(cfStore.getCompactionStrategyManager().onlyPurgeRepairedTombstones() && !Iterables.all(compacting, SSTableReader::isRepaired)) {
            return Collections.emptySet();
         } else {
            Set<SSTableReader> candidates = new HashSet();
            long minTimestamp = 9223372036854775807L;
            Iterator iterator = compacting.iterator();

            SSTableReader candidate;
            while(iterator.hasNext()) {
               candidate = (SSTableReader)iterator.next();
               if(candidate.getSSTableMetadata().maxLocalDeletionTime < gcBefore) {
                  candidates.add(candidate);
               } else {
                  minTimestamp = Math.min(minTimestamp, candidate.getMinTimestamp());
               }
            }

            if(ignoreOverlaps) {
               return candidates;
            } else {
               iterator = overlappingByKey.iterator();

               while(iterator.hasNext()) {
                  candidate = (SSTableReader)iterator.next();
                  if(candidate.getSSTableMetadata().maxLocalDeletionTime >= gcBefore) {
                     minTimestamp = Math.min(minTimestamp, candidate.getMinTimestamp());
                  }
               }

               Memtable memtable;
               for(iterator = cfStore.getTracker().getView().getAllMemtables().iterator(); iterator.hasNext(); minTimestamp = Math.min(minTimestamp, memtable.getMinTimestamp())) {
                  memtable = (Memtable)iterator.next();
               }

               iterator = candidates.iterator();

               while(iterator.hasNext()) {
                  candidate = (SSTableReader)iterator.next();
                  if(candidate.getMaxTimestamp() >= minTimestamp) {
                     iterator.remove();
                  } else {
                     logger.trace("Dropping expired SSTable {} (maxLocalDeletionTime={}, gcBefore={})", new Object[]{candidate, Integer.valueOf(candidate.getSSTableMetadata().maxLocalDeletionTime), Integer.valueOf(gcBefore)});
                  }
               }

               return candidates;
            }
         }
      } else {
         return Collections.emptySet();
      }
   }

   public String getKeyspace() {
      return this.cfs.keyspace.getName();
   }

   public String getColumnFamily() {
      return this.cfs.name;
   }

   public LongPredicate getPurgeEvaluator(final DecoratedKey key) {
      if (CompactionController.NEVER_PURGE_TOMBSTONES || !this.compactingRepaired()) {
         return time -> false;
      }
      this.overlapIterator.update(key);
      final Set<SSTableReader> filteredSSTables = this.overlapIterator.overlaps();
      final Iterable<Memtable> memtables = this.cfs.getTracker().getView().getAllMemtables();
      long minTimestampSeen = Long.MAX_VALUE;
      boolean hasTimestamp = false;
      for (final SSTableReader sstable : filteredSSTables) {
         if (sstable.couldContain(key)) {
            minTimestampSeen = Math.min(minTimestampSeen, sstable.getMinTimestamp());
            hasTimestamp = true;
         }
      }
      for (final Memtable memtable : memtables) {
         try {
            final Partition partition = memtable.getPartition(key).blockingLast(null);
            if (partition == null) {
               continue;
            }
            minTimestampSeen = Math.min(minTimestampSeen, partition.stats().minTimestamp);
            hasTimestamp = true;
         }
         catch (Exception e) {
            throw new RuntimeException(e);
         }
      }
      if (!hasTimestamp) {
         return time -> true;
      }
      final long finalTimestamp = minTimestampSeen;
      return time -> time < finalTimestamp;
   }


   public void close() {
      if(this.overlappingSSTables != null) {
         this.overlappingSSTables.release();
      }

      FileUtils.closeQuietly((Iterable)this.openDataFiles.values());
      this.openDataFiles.clear();
   }

   public boolean compactingRepaired() {
      return !this.cfs.getCompactionStrategyManager().onlyPurgeRepairedTombstones() || this.compactingRepaired;
   }

   boolean provideTombstoneSources() {
      return this.tombstoneOption != CompactionParams.TombstoneOption.NONE;
   }

   public Iterable<UnfilteredRowIterator> shadowSources(DecoratedKey key, boolean tombstoneOnly) {
      if(this.provideTombstoneSources() && this.compactingRepaired() && !NEVER_PURGE_TOMBSTONES) {
         this.overlapIterator.update(key);
         return Iterables.filter(Iterables.transform(this.overlapIterator.overlaps(), (reader) -> {
            return this.getShadowIterator(reader, key, tombstoneOnly);
         }), Predicates.notNull());
      } else {
         return null;
      }
   }

   private UnfilteredRowIterator getShadowIterator(SSTableReader reader, DecoratedKey key, boolean tombstoneOnly) {
      if(!reader.isMarkedSuspect() && reader.getMaxTimestamp() > this.minTimestamp && (!tombstoneOnly || reader.mayHaveTombstones())) {
         RowIndexEntry position = reader.getExactPosition(key);
         if(position == null) {
            return null;
         } else {
            FileDataInput dfile = (FileDataInput)this.openDataFiles.computeIfAbsent(reader, this::openDataFile);
            return reader.simpleIterator(dfile, key, position, tombstoneOnly);
         }
      } else {
         return null;
      }
   }

   private FileDataInput openDataFile(SSTableReader reader) {
      return this.limiter != null?reader.openDataReader(this.limiter, FileAccessType.FULL_FILE):reader.openDataReader(FileAccessType.FULL_FILE);
   }

   protected boolean ignoreOverlaps() {
      return this.ignoreOverlaps;
   }

   public Set<SSTableReader> getCompacting() {
      return (Set)(this.compacting != null?Sets.newHashSet(this.compacting):Collections.emptySet());
   }
}
