package org.apache.cassandra.db.compaction;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.utils.Pair;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @deprecated */
@Deprecated
public class DateTieredCompactionStrategy extends AbstractCompactionStrategy {
   private static final Logger logger = LoggerFactory.getLogger(DateTieredCompactionStrategy.class);
   private static final String DEPRECATED_WARNING = "DateTieredCompactionStrategy was enabled for '%s.%s' but it is deprecated  and its use is discouraged. It is recommended to use TimeWindowCompactionStrategy instead.";
   private final DateTieredCompactionStrategyOptions options;
   protected volatile int estimatedRemainingTasks = 0;
   private final Set<SSTableReader> sstables = new HashSet();
   private long lastExpiredCheck;
   private final SizeTieredCompactionStrategyOptions stcsOptions;

   public DateTieredCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options) {
      super(cfs, options);
      this.options = new DateTieredCompactionStrategyOptions(options);
      if(!options.containsKey("tombstone_compaction_interval") && !options.containsKey("tombstone_threshold")) {
         this.disableTombstoneCompactions = true;
         logger.trace("Disabling tombstone compactions for DTCS");
      } else {
         logger.trace("Enabling tombstone compactions for DTCS");
      }

      this.stcsOptions = new SizeTieredCompactionStrategyOptions(options);
   }

   public static void deprecatedWarning(String keyspace, String viewOrTable) {
      String warning = String.format("DateTieredCompactionStrategy was enabled for '%s.%s' but it is deprecated  and its use is discouraged. It is recommended to use TimeWindowCompactionStrategy instead.", new Object[]{keyspace, viewOrTable});
      logger.warn(warning);
      ClientWarn.instance.warn(warning);
   }

   public AbstractCompactionTask getNextBackgroundTask(int gcBefore) {
      List previousCandidate = null;

      while(true) {
         List<SSTableReader> latestBucket = this.getNextBackgroundSSTables(gcBefore);
         if(latestBucket.isEmpty()) {
            return null;
         }

         if(latestBucket.equals(previousCandidate)) {
            logger.warn("Could not acquire references for compacting SSTables {} which is not a problem per se,unless it happens frequently, in which case it must be reported. Will retry later.", latestBucket);
            return null;
         }

         LifecycleTransaction modifier = this.cfs.getTracker().tryModify((Iterable)latestBucket, OperationType.COMPACTION);
         if(modifier != null) {
            return new CompactionTask(this.cfs, modifier, gcBefore);
         }

         previousCandidate = latestBucket;
      }
   }

   private synchronized List<SSTableReader> getNextBackgroundSSTables(int gcBefore) {
      if(this.sstables.isEmpty()) {
         return Collections.emptyList();
      } else {
         Iterable var10000 = this.cfs.getUncompactingSSTables();
         Set var10001 = this.sstables;
         this.sstables.getClass();
         Set<SSTableReader> uncompacting = ImmutableSet.copyOf(Iterables.filter(var10000, var10001::contains));
         Set<SSTableReader> expired = Collections.emptySet();
         if(System.currentTimeMillis() - this.lastExpiredCheck > this.options.expiredSSTableCheckFrequency) {
            expired = CompactionController.getFullyExpiredSSTables(this.cfs, uncompacting, this.cfs.getOverlappingLiveSSTables(uncompacting), gcBefore);
            this.lastExpiredCheck = System.currentTimeMillis();
         }

         Set<SSTableReader> candidates = Sets.newHashSet(filterSuspectSSTables(uncompacting));
         List<SSTableReader> compactionCandidates = new ArrayList(this.getNextNonExpiredSSTables(Sets.difference(candidates, expired), gcBefore));
         if(!expired.isEmpty()) {
            logger.trace("Including expired sstables: {}", expired);
            compactionCandidates.addAll(expired);
         }

         return compactionCandidates;
      }
   }

   private List<SSTableReader> getNextNonExpiredSSTables(Iterable<SSTableReader> nonExpiringSSTables, int gcBefore) {
      int base = this.cfs.getMinimumCompactionThreshold();
      long now = this.getNow();
      List<SSTableReader> mostInteresting = this.getCompactionCandidates(nonExpiringSSTables, now, base);
      if(mostInteresting != null) {
         return mostInteresting;
      } else {
         List<SSTableReader> sstablesWithTombstones = Lists.newArrayList();
         Iterator var8 = nonExpiringSSTables.iterator();

         while(var8.hasNext()) {
            SSTableReader sstable = (SSTableReader)var8.next();
            if(this.worthDroppingTombstones(sstable, gcBefore)) {
               sstablesWithTombstones.add(sstable);
            }
         }

         if(sstablesWithTombstones.isEmpty()) {
            return Collections.emptyList();
         } else {
            return Collections.singletonList(Collections.min(sstablesWithTombstones, SSTableReader.sizeComparator));
         }
      }
   }

   private List<SSTableReader> getCompactionCandidates(Iterable<SSTableReader> candidateSSTables, long now, int base) {
      Iterable<SSTableReader> candidates = filterOldSSTables(Lists.newArrayList(candidateSSTables), this.options.maxSSTableAge, now);
      List<List<SSTableReader>> buckets = getBuckets(createSSTableAndMinTimestampPairs(candidates), this.options.baseTime, base, now, this.options.maxWindowSize);
      logger.debug("Compaction buckets are {}", buckets);
      this.updateEstimatedCompactionsByTasks(buckets);
      List<SSTableReader> mostInteresting = newestBucket(buckets, this.cfs.getMinimumCompactionThreshold(), this.cfs.getMaximumCompactionThreshold(), now, this.options.baseTime, this.options.maxWindowSize, this.stcsOptions);
      return !mostInteresting.isEmpty()?mostInteresting:null;
   }

   private long getNow() {
      List<SSTableReader> list = new ArrayList();
      Iterables.addAll(list, this.cfs.getSSTables(SSTableSet.LIVE));
      return list.isEmpty()?0L:((SSTableReader)Collections.max(list, (o1, o2) -> {
         return Long.compare(o1.getMaxTimestamp(), o2.getMaxTimestamp());
      })).getMaxTimestamp();
   }

   @VisibleForTesting
   static Iterable<SSTableReader> filterOldSSTables(List<SSTableReader> sstables, long maxSSTableAge, long now) {
      if(maxSSTableAge == 0L) {
         return sstables;
      } else {
         final long cutoff = now - maxSSTableAge;
         return Iterables.filter(sstables, new Predicate<SSTableReader>() {
            public boolean apply(SSTableReader sstable) {
               return sstable.getMaxTimestamp() >= cutoff;
            }
         });
      }
   }

   public static List<Pair<SSTableReader, Long>> createSSTableAndMinTimestampPairs(Iterable<SSTableReader> sstables) {
      List<Pair<SSTableReader, Long>> sstableMinTimestampPairs = Lists.newArrayListWithCapacity(Iterables.size(sstables));
      Iterator var2 = sstables.iterator();

      while(var2.hasNext()) {
         SSTableReader sstable = (SSTableReader)var2.next();
         sstableMinTimestampPairs.add(Pair.create(sstable, Long.valueOf(sstable.getMinTimestamp())));
      }

      return sstableMinTimestampPairs;
   }

   public void addSSTable(SSTableReader sstable) {
      this.sstables.add(sstable);
   }

   public void removeSSTable(SSTableReader sstable) {
      this.sstables.remove(sstable);
   }

   protected Set<SSTableReader> getSSTables() {
      return ImmutableSet.copyOf(this.sstables);
   }

   @VisibleForTesting
   static <T> List<List<T>> getBuckets(Collection<Pair<T, Long>> files, long timeUnit, int base, long now, long maxWindowSize) {
      List<Pair<T, Long>> sortedFiles = Lists.newArrayList(files);
      Collections.sort(sortedFiles, Collections.reverseOrder(new Comparator<Pair<T, Long>>() {
         public int compare(Pair<T, Long> p1, Pair<T, Long> p2) {
            return ((Long)p1.right).compareTo((Long)p2.right);
         }
      }));
      List<List<T>> buckets = Lists.newArrayList();
      DateTieredCompactionStrategy.Target target = getInitialTarget(now, timeUnit, maxWindowSize);

      ArrayList bucket;
      for(PeekingIterator it = Iterators.peekingIterator(sortedFiles.iterator()); it.hasNext(); buckets.add(bucket)) {
         while(!target.onTarget(((Long)((Pair)it.peek()).right).longValue())) {
            if(target.compareToTimestamp(((Long)((Pair)it.peek()).right).longValue()) < 0) {
               it.next();
               if(!it.hasNext()) {
                  return buckets;
               }
            } else {
               target = target.nextTarget(base);
            }
         }

         bucket = Lists.newArrayList();

         while(target.onTarget(((Long)((Pair)it.peek()).right).longValue())) {
            bucket.add(((Pair)it.next()).left);
            if(!it.hasNext()) {
               break;
            }
         }
      }

      return buckets;
   }

   @VisibleForTesting
   static DateTieredCompactionStrategy.Target getInitialTarget(long now, long timeUnit, long maxWindowSize) {
      return new DateTieredCompactionStrategy.Target(timeUnit, now / timeUnit, maxWindowSize);
   }

   private void updateEstimatedCompactionsByTasks(List<List<SSTableReader>> tasks) {
      int n = 0;
      Iterator var3 = tasks.iterator();

      while(var3.hasNext()) {
         List<SSTableReader> bucket = (List)var3.next();
         Iterator var5 = getSTCSBuckets(bucket, this.stcsOptions).iterator();

         while(var5.hasNext()) {
            List<SSTableReader> stcsBucket = (List)var5.next();
            if(stcsBucket.size() >= this.cfs.getMinimumCompactionThreshold()) {
               n = (int)((double)n + Math.ceil((double)stcsBucket.size() / (double)this.cfs.getMaximumCompactionThreshold()));
            }
         }
      }

      this.estimatedRemainingTasks = n;
      this.cfs.getCompactionStrategyManager().compactionLogger.pending(this, n);
   }

   @VisibleForTesting
   static List<SSTableReader> newestBucket(List<List<SSTableReader>> buckets, int minThreshold, int maxThreshold, long now, long baseTime, long maxWindowSize, SizeTieredCompactionStrategyOptions stcsOptions) {
      DateTieredCompactionStrategy.Target incomingWindow = getInitialTarget(now, baseTime, maxWindowSize);
      Iterator var11 = buckets.iterator();

      List stcsSSTables;
      do {
         List bucket;
         boolean inFirstWindow;
         do {
            if(!var11.hasNext()) {
               return Collections.emptyList();
            }

            bucket = (List)var11.next();
            inFirstWindow = incomingWindow.onTarget(((SSTableReader)bucket.get(0)).getMinTimestamp());
         } while(bucket.size() < minThreshold && (bucket.size() < 2 || inFirstWindow));

         stcsSSTables = getSSTablesForSTCS(bucket, inFirstWindow?minThreshold:2, maxThreshold, stcsOptions);
      } while(stcsSSTables.isEmpty());

      return stcsSSTables;
   }

   private static List<SSTableReader> getSSTablesForSTCS(Collection<SSTableReader> sstables, int minThreshold, int maxThreshold, SizeTieredCompactionStrategyOptions stcsOptions) {
      List<SSTableReader> s = SizeTieredCompactionStrategy.mostInterestingBucket(getSTCSBuckets(sstables, stcsOptions), minThreshold, maxThreshold);
      logger.debug("Got sstables {} for STCS from {}", s, sstables);
      return s;
   }

   private static List<List<SSTableReader>> getSTCSBuckets(Collection<SSTableReader> sstables, SizeTieredCompactionStrategyOptions stcsOptions) {
      List<Pair<SSTableReader, Long>> pairs = SizeTieredCompactionStrategy.createSSTableAndLengthPairs(AbstractCompactionStrategy.filterSuspectSSTables(sstables));
      return SizeTieredCompactionStrategy.getBuckets(pairs, stcsOptions.bucketHigh, stcsOptions.bucketLow, stcsOptions.minSSTableSize);
   }

   public synchronized Collection<AbstractCompactionTask> getMaximalTask(int gcBefore, boolean splitOutput) {
      Iterable<SSTableReader> filteredSSTables = filterSuspectSSTables(this.sstables);
      if(Iterables.isEmpty(filteredSSTables)) {
         return null;
      } else {
         LifecycleTransaction txn = this.cfs.getTracker().tryModify(filteredSSTables, OperationType.COMPACTION);
         return txn == null?null:Collections.singleton(new CompactionTask(this.cfs, txn, gcBefore));
      }
   }

   public synchronized AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, int gcBefore) {
      assert !sstables.isEmpty();

      LifecycleTransaction modifier = this.cfs.getTracker().tryModify((Iterable)sstables, OperationType.COMPACTION);
      if(modifier == null) {
         logger.trace("Unable to mark {} for compaction; probably a background compaction got to it first.  You can disable background compactions temporarily if this is a problem", sstables);
         return null;
      } else {
         return (new CompactionTask(this.cfs, modifier, gcBefore)).setUserDefined(true);
      }
   }

   public int getEstimatedRemainingTasks() {
      return this.estimatedRemainingTasks;
   }

   public long getMaxSSTableBytes() {
      return 9223372036854775807L;
   }

   public Collection<Collection<SSTableReader>> groupSSTablesForAntiCompaction(Collection<SSTableReader> sstablesToGroup) {
      Collection<Collection<SSTableReader>> groups = new ArrayList(sstablesToGroup.size());
      Iterator var3 = sstablesToGroup.iterator();

      while(var3.hasNext()) {
         SSTableReader sstable = (SSTableReader)var3.next();
         groups.add(Collections.singleton(sstable));
      }

      return groups;
   }

   public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException {
      Map<String, String> uncheckedOptions = AbstractCompactionStrategy.validateOptions(options);
      uncheckedOptions = DateTieredCompactionStrategyOptions.validateOptions(options, uncheckedOptions);
      uncheckedOptions.remove(CompactionParams.Option.MIN_THRESHOLD.toString());
      uncheckedOptions.remove(CompactionParams.Option.MAX_THRESHOLD.toString());
      uncheckedOptions = SizeTieredCompactionStrategyOptions.validateOptions(options, uncheckedOptions);
      return uncheckedOptions;
   }

   public CompactionLogger.Strategy strategyLogger() {
      return new CompactionLogger.Strategy() {
         public JsonNode sstable(SSTableReader sstable) {
            ObjectNode node = JsonNodeFactory.instance.objectNode();
            node.put("min_timestamp", sstable.getMinTimestamp());
            node.put("max_timestamp", sstable.getMaxTimestamp());
            return node;
         }

         public JsonNode options() {
            ObjectNode node = JsonNodeFactory.instance.objectNode();
            TimeUnit resolution = DateTieredCompactionStrategy.this.options.timestampResolution;
            node.put("timestamp_resolution", resolution.toString());
            node.put("base_time_seconds", resolution.toSeconds(DateTieredCompactionStrategy.this.options.baseTime));
            node.put("max_window_size_seconds", resolution.toSeconds(DateTieredCompactionStrategy.this.options.maxWindowSize));
            return node;
         }
      };
   }

   public String toString() {
      return String.format("DateTieredCompactionStrategy[%s/%s]", new Object[]{Integer.valueOf(this.cfs.getMinimumCompactionThreshold()), Integer.valueOf(this.cfs.getMaximumCompactionThreshold())});
   }

   private static class Target {
      public final long size;
      public final long divPosition;
      public final long maxWindowSize;

      public Target(long size, long divPosition, long maxWindowSize) {
         this.size = size;
         this.divPosition = divPosition;
         this.maxWindowSize = maxWindowSize;
      }

      public int compareToTimestamp(long timestamp) {
         return Long.compare(this.divPosition, timestamp / this.size);
      }

      public boolean onTarget(long timestamp) {
         return this.compareToTimestamp(timestamp) == 0;
      }

      public DateTieredCompactionStrategy.Target nextTarget(int base) {
         return this.divPosition % (long)base <= 0L && this.size * (long)base <= this.maxWindowSize?new DateTieredCompactionStrategy.Target(this.size * (long)base, this.divPosition / (long)base - 1L, this.maxWindowSize):new DateTieredCompactionStrategy.Target(this.size, this.divPosition - 1L, this.maxWindowSize);
      }
   }
}
