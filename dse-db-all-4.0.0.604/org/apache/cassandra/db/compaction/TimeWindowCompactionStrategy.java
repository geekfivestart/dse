package org.apache.cassandra.db.compaction;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeWindowCompactionStrategy extends AbstractCompactionStrategy {
    private static final Logger logger = LoggerFactory.getLogger(TimeWindowCompactionStrategy.class);
    private final TimeWindowCompactionStrategyOptions options;
    protected volatile int estimatedRemainingTasks = 0;
    private final Set<SSTableReader> sstables = new HashSet();
    private long lastExpiredCheck;
    private long highestWindowSeen;

    public TimeWindowCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options) {
        super(cfs, options);
        this.options = new TimeWindowCompactionStrategyOptions(options);
        if (!options.containsKey("tombstone_compaction_interval") && !options.containsKey("tombstone_threshold")) {
            this.disableTombstoneCompactions = true;
            logger.debug("Disabling tombstone compactions for TWCS");
        } else {
            logger.debug("Enabling tombstone compactions for TWCS");
        }

    }

    public AbstractCompactionTask getNextBackgroundTask(int gcBefore) {
        List previousCandidate = null;

        while (true) {
            List<SSTableReader> latestBucket = this.getNextBackgroundSSTables(gcBefore);
            if (latestBucket.isEmpty()) {
                return null;
            }

            if (latestBucket.equals(previousCandidate)) {
                logger.warn("Could not acquire references for compacting SSTables {} which is not a problem per se,unless it happens frequently, in which case it must be reported. Will retry later.", latestBucket);
                return null;
            }

            LifecycleTransaction modifier = this.cfs.getTracker().tryModify((Iterable) latestBucket, OperationType.COMPACTION);
            if (modifier != null) {
                return new CompactionTask(this.cfs, modifier, gcBefore, this.options.ignoreOverlaps);
            }

            previousCandidate = latestBucket;
        }
    }

    private synchronized List<SSTableReader> getNextBackgroundSSTables(int gcBefore) {
        if (Iterables.isEmpty(this.cfs.getSSTables(SSTableSet.LIVE))) {
            return Collections.emptyList();
        } else {
            Iterable var10000 = this.cfs.getUncompactingSSTables();
            Set var10001 = this.sstables;
            this.sstables.getClass();
            Set<SSTableReader> uncompacting = ImmutableSet.copyOf(Iterables.filter(var10000, var10001::contains));
            Set<SSTableReader> expired = Collections.emptySet();
            if (System.currentTimeMillis() - this.lastExpiredCheck > this.options.expiredSSTableCheckFrequency) {
                logger.debug("TWCS expired check sufficiently far in the past, checking for fully expired SSTables");
                expired = CompactionController.getFullyExpiredSSTables(this.cfs, uncompacting, this.cfs.getOverlappingLiveSSTables(uncompacting), gcBefore, this.options.ignoreOverlaps);
                this.lastExpiredCheck = System.currentTimeMillis();
            } else {
                logger.debug("TWCS skipping check for fully expired SSTables");
            }

            Set<SSTableReader> candidates = Sets.newHashSet(filterSuspectSSTables(uncompacting));
            List<SSTableReader> compactionCandidates = new ArrayList(this.getNextNonExpiredSSTables(Sets.difference(candidates, expired), gcBefore));
            if (!expired.isEmpty()) {
                logger.debug("Including expired sstables: {}", expired);
                compactionCandidates.addAll(expired);
            }

            return compactionCandidates;
        }
    }

    private List<SSTableReader> getNextNonExpiredSSTables(Iterable<SSTableReader> nonExpiringSSTables, int gcBefore) {
        List<SSTableReader> mostInteresting = this.getCompactionCandidates(nonExpiringSSTables);
        if (mostInteresting != null) {
            return mostInteresting;
        } else {
            List<SSTableReader> sstablesWithTombstones = new ArrayList();
            Iterator var5 = nonExpiringSSTables.iterator();

            while (var5.hasNext()) {
                SSTableReader sstable = (SSTableReader) var5.next();
                if (this.worthDroppingTombstones(sstable, gcBefore)) {
                    sstablesWithTombstones.add(sstable);
                }
            }

            if (sstablesWithTombstones.isEmpty()) {
                return Collections.emptyList();
            } else {
                return Collections.singletonList(Collections.min(sstablesWithTombstones, SSTableReader.sizeComparator));
            }
        }
    }

    private List<SSTableReader> getCompactionCandidates(Iterable<SSTableReader> candidateSSTables) {
        Pair<HashMultimap<Long, SSTableReader>, Long> buckets = getBuckets(candidateSSTables, this.options.sstableWindowUnit, this.options.sstableWindowSize, this.options.timestampResolution);
        if (((Long) buckets.right).longValue() > this.highestWindowSeen) {
            this.highestWindowSeen = ((Long) buckets.right).longValue();
        }

        this.updateEstimatedCompactionsByTasks((HashMultimap) buckets.left);
        List<SSTableReader> mostInteresting = newestBucket((HashMultimap) buckets.left, this.cfs.getMinimumCompactionThreshold(), this.cfs.getMaximumCompactionThreshold(), this.options.stcsOptions, this.highestWindowSeen);
        return !mostInteresting.isEmpty() ? mostInteresting : null;
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

    public static Pair<Long, Long> getWindowBoundsInMillis(TimeUnit windowTimeUnit, int windowTimeSize, long timestampInMillis) {
        long upperTimestamp;
        long lowerTimestamp;
        long timestampInSeconds = TimeUnit.SECONDS.convert(timestampInMillis, TimeUnit.MILLISECONDS);
        switch (windowTimeUnit) {
            case MINUTES: {
                lowerTimestamp = timestampInSeconds - timestampInSeconds % (60L * (long) windowTimeSize);
                upperTimestamp = lowerTimestamp + 60L * ((long) windowTimeSize - 1L) + 59L;
                break;
            }
            case HOURS: {
                lowerTimestamp = timestampInSeconds - timestampInSeconds % (3600L * (long) windowTimeSize);
                upperTimestamp = lowerTimestamp + 3600L * ((long) windowTimeSize - 1L) + 3599L;
                break;
            }
            default: {
                lowerTimestamp = timestampInSeconds - timestampInSeconds % (86400L * (long) windowTimeSize);
                upperTimestamp = lowerTimestamp + 86400L * ((long) windowTimeSize - 1L) + 86399L;
            }
        }
        return Pair.create(TimeUnit.MILLISECONDS.convert(lowerTimestamp, TimeUnit.SECONDS), TimeUnit.MILLISECONDS.convert(upperTimestamp, TimeUnit.SECONDS));
    }

    @VisibleForTesting
    static Pair<HashMultimap<Long, SSTableReader>, Long> getBuckets(Iterable<SSTableReader> files, TimeUnit sstableWindowUnit, int sstableWindowSize, TimeUnit timestampResolution) {
        HashMultimap<Long, SSTableReader> buckets = HashMultimap.create();
        long maxTimestamp = 0L;
        Iterator var7 = files.iterator();

        while (var7.hasNext()) {
            SSTableReader f = (SSTableReader) var7.next();

            assert TimeWindowCompactionStrategyOptions.validTimestampTimeUnits.contains(timestampResolution);

            long tStamp = TimeUnit.MILLISECONDS.convert(f.getMaxTimestamp(), timestampResolution);
            Pair<Long, Long> bounds = getWindowBoundsInMillis(sstableWindowUnit, sstableWindowSize, tStamp);
            buckets.put(bounds.left, f);
            if (((Long) bounds.left).longValue() > maxTimestamp) {
                maxTimestamp = ((Long) bounds.left).longValue();
            }
        }

        logger.trace("buckets {}, max timestamp {}", buckets, Long.valueOf(maxTimestamp));
        return Pair.create(buckets, Long.valueOf(maxTimestamp));
    }

    private void updateEstimatedCompactionsByTasks(HashMultimap<Long, SSTableReader> tasks) {
        int n = 0;
        long now = this.highestWindowSeen;
        Iterator var5 = tasks.keySet().iterator();

        while (true) {
            while (var5.hasNext()) {
                Long key = (Long) var5.next();
                if (key.compareTo(Long.valueOf(now)) >= 0 && tasks.get(key).size() >= this.cfs.getMinimumCompactionThreshold()) {
                    ++n;
                } else if (key.compareTo(Long.valueOf(now)) < 0 && tasks.get(key).size() >= 2) {
                    ++n;
                }
            }

            this.estimatedRemainingTasks = n;
            return;
        }
    }

    @VisibleForTesting
    static List<SSTableReader> newestBucket(HashMultimap<Long, SSTableReader> buckets, int minThreshold, int maxThreshold, SizeTieredCompactionStrategyOptions stcsOptions, long now) {
        TreeSet<Long> allKeys = new TreeSet(buckets.keySet());
        Iterator it = allKeys.descendingIterator();

        List stcsInterestingBucket;
        label29:
        do {
            while (it.hasNext()) {
                Long key = (Long) it.next();
                Set<SSTableReader> bucket = buckets.get(key);
                logger.trace("Key {}, now {}", key, Long.valueOf(now));
                if (bucket.size() >= minThreshold && key.longValue() >= now) {
                    List<Pair<SSTableReader, Long>> pairs = SizeTieredCompactionStrategy.createSSTableAndLengthPairs(bucket);
                    List<List<SSTableReader>> stcsBuckets = SizeTieredCompactionStrategy.getBuckets(pairs, stcsOptions.bucketHigh, stcsOptions.bucketLow, stcsOptions.minSSTableSize);
                    logger.debug("Using STCS compaction for first window of bucket: data files {} , options {}", pairs, stcsOptions);
                    stcsInterestingBucket = SizeTieredCompactionStrategy.mostInterestingBucket(stcsBuckets, minThreshold, maxThreshold);
                    continue label29;
                }

                if (bucket.size() >= 2 && key.longValue() < now) {
                    logger.debug("bucket size {} >= 2 and not in current bucket, compacting what's here: {}", Integer.valueOf(bucket.size()), bucket);
                    return trimToThreshold(bucket, maxThreshold);
                }

                logger.trace("No compaction necessary for bucket size {} , key {}, now {}", new Object[]{Integer.valueOf(bucket.size()), key, Long.valueOf(now)});
            }

            return Collections.emptyList();
        } while (stcsInterestingBucket.isEmpty());

        return stcsInterestingBucket;
    }

    @VisibleForTesting
    static List<SSTableReader> trimToThreshold(Set<SSTableReader> bucket, int maxThreshold) {
        List<SSTableReader> ssTableReaders = new ArrayList(bucket);
        Collections.sort(ssTableReaders, SSTableReader.sizeComparator);
        return ImmutableList.copyOf(Iterables.limit(ssTableReaders, maxThreshold));
    }

    public synchronized Collection<AbstractCompactionTask> getMaximalTask(int gcBefore, boolean splitOutput) {
        Iterable<SSTableReader> filteredSSTables = filterSuspectSSTables(this.sstables);
        if (Iterables.isEmpty(filteredSSTables)) {
            return null;
        } else {
            LifecycleTransaction txn = this.cfs.getTracker().tryModify(filteredSSTables, OperationType.COMPACTION);
            return txn == null ? null : Collections.singleton(new CompactionTask(this.cfs, txn, gcBefore, this.options.ignoreOverlaps));
        }
    }

    public synchronized AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, int gcBefore) {
        assert !sstables.isEmpty();

        LifecycleTransaction modifier = this.cfs.getTracker().tryModify((Iterable) sstables, OperationType.COMPACTION);
        if (modifier == null) {
            logger.debug("Unable to mark {} for compaction; probably a background compaction got to it first.  You can disable background compactions temporarily if this is a problem", sstables);
            return null;
        } else {
            return (new CompactionTask(this.cfs, modifier, gcBefore, this.options.ignoreOverlaps)).setUserDefined(true);
        }
    }

    public int getEstimatedRemainingTasks() {
        return this.estimatedRemainingTasks;
    }

    public long getMaxSSTableBytes() {
        return 9223372036854775807L;
    }

    public SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor, long keyCount, long repairedAt, UUID pendingRepair, MetadataCollector meta, SerializationHeader header, Collection<Index> indexes, LifecycleTransaction txn) {
        return (SSTableMultiWriter) (this.options.splitDuringFlush ? new TWCSMultiWriter(this.cfs, this.options.sstableWindowUnit, this.options.sstableWindowSize, this.options.timestampResolution, descriptor, keyCount, repairedAt, pendingRepair, meta, header, indexes, txn) : super.createSSTableMultiWriter(descriptor, keyCount, repairedAt, pendingRepair, meta, header, indexes, txn));
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException {
        Map<String, String> uncheckedOptions = AbstractCompactionStrategy.validateOptions(options);
        uncheckedOptions = TimeWindowCompactionStrategyOptions.validateOptions(options, uncheckedOptions);
        uncheckedOptions.remove(CompactionParams.Option.MIN_THRESHOLD.toString());
        uncheckedOptions.remove(CompactionParams.Option.MAX_THRESHOLD.toString());
        return uncheckedOptions;
    }

    public String toString() {
        return String.format("TimeWindowCompactionStrategy[%s/%s]", new Object[]{Integer.valueOf(this.cfs.getMinimumCompactionThreshold()), Integer.valueOf(this.cfs.getMaximumCompactionThreshold())});
    }

    private static boolean isSplitDuringFlushEnabled(Map<String, String> options) {
        return Boolean.parseBoolean((String) options.get("split_during_flush"));
    }

    public static String getNodeSyncSplitDuringFlushWarning(String keyspace, String columnFamily) {
        return String.format("NodeSync is enabled on table %s.%s with TimeWindowCompactionStrategy (TWCS) but the '%s' option is disabled. We recommend enabling the '%s' option when using TWCS in conjunction with NodeSync to ensure data repaired by NodeSync is placed in the correct TWCS time window.", new Object[]{keyspace, columnFamily, "split_during_flush", "split_during_flush"});
    }

    public static boolean shouldLogNodeSyncSplitDuringFlushWarning(TableMetadata tableMetadata, TableParams tableParams) {
        return tableParams.compaction.klass().equals(TimeWindowCompactionStrategy.class) && tableParams.nodeSync.isEnabled(tableMetadata) && !isSplitDuringFlushEnabled(tableParams.compaction.options());
    }
}
