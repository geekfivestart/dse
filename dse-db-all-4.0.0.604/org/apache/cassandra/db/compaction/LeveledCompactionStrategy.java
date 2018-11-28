package org.apache.cassandra.db.compaction;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.primitives.Doubles;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeveledCompactionStrategy extends AbstractCompactionStrategy {
    private static final Logger logger = LoggerFactory.getLogger(LeveledCompactionStrategy.class);
    private static final String SSTABLE_SIZE_OPTION = "sstable_size_in_mb";
    private static final boolean tolerateSstableSize = Boolean.getBoolean("cassandra.tolerate_sstable_size");
    private static final String LEVEL_FANOUT_SIZE_OPTION = "fanout_size";
    public static final int DEFAULT_LEVEL_FANOUT_SIZE = 10;
    @VisibleForTesting
    final LeveledManifest manifest;
    private final int maxSSTableSizeInMB;
    private final int levelFanoutSize;

    public LeveledCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options) {
        super(cfs, options);
        int configuredMaxSSTableSize = 160;
        int configuredLevelFanoutSize = 10;
        SizeTieredCompactionStrategyOptions localOptions = new SizeTieredCompactionStrategyOptions(options);
        if (options != null) {
            if (options.containsKey("sstable_size_in_mb")) {
                configuredMaxSSTableSize = Integer.parseInt((String) options.get("sstable_size_in_mb"));
                if (!tolerateSstableSize) {
                    if (configuredMaxSSTableSize >= 1000) {
                        logger.warn("Max sstable size of {}MB is configured for {}.{}; having a unit of compaction this large is probably a bad idea", new Object[]{Integer.valueOf(configuredMaxSSTableSize), cfs.name, cfs.getTableName()});
                    }

                    if (configuredMaxSSTableSize < 50) {
                        logger.warn("Max sstable size of {}MB is configured for {}.{}.  Testing done for CASSANDRA-5727 indicates that performance improves up to 160MB", new Object[]{Integer.valueOf(configuredMaxSSTableSize), cfs.name, cfs.getTableName()});
                    }
                }
            }

            if (options.containsKey("fanout_size")) {
                configuredLevelFanoutSize = Integer.parseInt((String) options.get("fanout_size"));
            }
        }

        this.maxSSTableSizeInMB = configuredMaxSSTableSize;
        this.levelFanoutSize = configuredLevelFanoutSize;
        this.manifest = new LeveledManifest(cfs, this.maxSSTableSizeInMB, this.levelFanoutSize, localOptions);
        logger.trace("Created {}", this.manifest);
    }

    public int getLevelSize(int i) {
        return this.manifest.getLevelSize(i);
    }

    public int[] getAllLevelSize() {
        return this.manifest.getAllLevelSize();
    }

    public void startup() {
        this.manifest.calculateLastCompactedKeys();
        super.startup();
    }

    public AbstractCompactionTask getNextBackgroundTask(int gcBefore) {
        Collection previousCandidate = null;

        while (true) {
            LeveledManifest.CompactionCandidate candidate = this.manifest.getCompactionCandidates();
            OperationType op;
            if (candidate == null) {
                SSTableReader sstable = this.findDroppableSSTable(gcBefore);
                if (sstable == null) {
                    logger.trace("No compaction necessary for {}", this);
                    return null;
                }

                candidate = new LeveledManifest.CompactionCandidate(Collections.singleton(sstable), sstable.getSSTableLevel(), this.getMaxSSTableBytes());
                op = OperationType.TOMBSTONE_COMPACTION;
            } else {
                op = OperationType.COMPACTION;
            }

            if (candidate.sstables.equals(previousCandidate)) {
                logger.warn("Could not acquire references for compacting SSTables {} which is not a problem per se,unless it happens frequently, in which case it must be reported. Will retry later.", candidate.sstables);
                return null;
            }

            LifecycleTransaction txn = this.cfs.getTracker().tryModify((Iterable) candidate.sstables, OperationType.COMPACTION);
            if (txn != null) {
                LeveledCompactionTask newTask = new LeveledCompactionTask(this.cfs, txn, candidate.level, gcBefore, candidate.maxSSTableBytes, false);
                newTask.setCompactionType(op);
                return newTask;
            }

            previousCandidate = candidate.sstables;
        }
    }

    public synchronized Collection<AbstractCompactionTask> getMaximalTask(int gcBefore, boolean splitOutput) {
        Iterable<SSTableReader> sstables = this.manifest.getAllSSTables();
        Iterable<SSTableReader> filteredSSTables = filterSuspectSSTables(sstables);
        if (Iterables.isEmpty(sstables)) {
            return null;
        } else {
            LifecycleTransaction txn = this.cfs.getTracker().tryModify(filteredSSTables, OperationType.COMPACTION);
            return txn == null ? null : Arrays.asList(new AbstractCompactionTask[]{new LeveledCompactionTask(this.cfs, txn, 0, gcBefore, this.getMaxSSTableBytes(), true)});
        }
    }

    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, int gcBefore) {
        if (sstables.isEmpty()) {
            return null;
        } else {
            LifecycleTransaction transaction = this.cfs.getTracker().tryModify((Iterable) sstables, OperationType.COMPACTION);
            if (transaction == null) {
                logger.trace("Unable to mark {} for compaction; probably a background compaction got to it first.  You can disable background compactions temporarily if this is a problem", sstables);
                return null;
            } else {
                int level = sstables.size() > 1 ? 0 : ((SSTableReader) sstables.iterator().next()).getSSTableLevel();
                return new LeveledCompactionTask(this.cfs, transaction, level, gcBefore, level == 0 ? 9223372036854775807L : this.getMaxSSTableBytes(), false);
            }
        }
    }

    public AbstractCompactionTask getCompactionTask(LifecycleTransaction txn, int gcBefore, long maxSSTableBytes) {
        assert txn.originals().size() > 0;

        int level = -1;
        Iterator var6 = txn.originals().iterator();

        while (var6.hasNext()) {
            SSTableReader sstable = (SSTableReader) var6.next();
            if (level == -1) {
                level = sstable.getSSTableLevel();
            }

            if (level != sstable.getSSTableLevel()) {
                level = 0;
            }
        }

        return new LeveledCompactionTask(this.cfs, txn, level, gcBefore, maxSSTableBytes, false);
    }

    public Collection<Collection<SSTableReader>> groupSSTablesForAntiCompaction(Collection<SSTableReader> ssTablesToGroup) {
        int groupSize = 2;
        HashMap<Integer, ArrayList<SSTableReader>> sstablesByLevel = new HashMap<Integer, ArrayList<SSTableReader>>();
        for (SSTableReader sstable : ssTablesToGroup) {
            Integer level = sstable.getSSTableLevel();
            ArrayList<SSTableReader> sstablesForLevel = (ArrayList<SSTableReader>) sstablesByLevel.get(level);
            if (sstablesForLevel == null) {
                sstablesForLevel = new ArrayList<SSTableReader>();
                sstablesByLevel.put(level, sstablesForLevel);
            }
            sstablesForLevel.add(sstable);
        }
        ArrayList<Collection<SSTableReader>> groupedSSTables = new ArrayList<Collection<SSTableReader>>();
        for (Collection<SSTableReader> levelOfSSTables : sstablesByLevel.values()) {
            ArrayList<SSTableReader> currGroup = new ArrayList<SSTableReader>(groupSize);
            for (SSTableReader sstable : levelOfSSTables) {
                currGroup.add(sstable);
                if (currGroup.size() != groupSize) continue;
                groupedSSTables.add(currGroup);
                currGroup = new ArrayList(groupSize);
            }
            if (currGroup.size() == 0) continue;
            groupedSSTables.add(currGroup);
        }
        return groupedSSTables;
    }

    public int getEstimatedRemainingTasks() {
        int n = this.manifest.getEstimatedTasks();
        this.cfs.getCompactionStrategyManager().compactionLogger.pending(this, n);
        return n;
    }

    public long getMaxSSTableBytes() {
        return (long) this.maxSSTableSizeInMB * 1024L * 1024L;
    }

    public int getLevelFanoutSize() {
        return this.levelFanoutSize;
    }

    public AbstractCompactionStrategy.ScannerList getScanners(Collection<SSTableReader> sstables, Collection<Range<Token>> ranges) {
        Set<SSTableReader>[] sstablesPerLevel = this.manifest.getSStablesPerLevelSnapshot();
        ArrayListMultimap<Integer, SSTableReader> byLevel = ArrayListMultimap.create();
        for (SSTableReader sstable : sstables) {
            int level = sstable.getSSTableLevel();
            if (level >= sstablesPerLevel.length || !sstablesPerLevel[level].contains(sstable)) {
                logger.warn("Live sstable {} from level {} is not on corresponding level in the leveled manifest. This is not a problem per se, but may indicate an orphaned sstable due to a failed compaction not cleaned up properly.", (Object) sstable.getFilename(), (Object) level);
                level = -1;
            }
            byLevel.get(level).add(sstable);
        }
        ArrayList<ISSTableScanner> scanners = new ArrayList<ISSTableScanner>(sstables.size());
        try {
            for (Integer level : byLevel.keySet()) {
                if (level <= 0) {
                    for (SSTableReader sstable : byLevel.get(level)) {
                        scanners.add(sstable.getScanner(ranges));
                    }
                    continue;
                }
                Collection<SSTableReader> intersecting = LeveledScanner.intersecting(byLevel.get(level), ranges);
                if (intersecting.isEmpty()) continue;
                LeveledScanner scanner = new LeveledScanner(this.cfs.metadata(), intersecting, ranges);
                scanners.add(scanner);
            }
        } catch (Throwable t) {
            ISSTableScanner.closeAllAndPropagate(scanners, t);
        }
        return new AbstractCompactionStrategy.ScannerList(scanners);
    }

    public void replaceSSTables(Collection<SSTableReader> removed, Collection<SSTableReader> added) {
        this.manifest.replace(removed, added);
    }

    public void addSSTable(SSTableReader added) {
        this.manifest.add(added, false);
    }

    public void addSSTableFromStreaming(SSTableReader added) {
        this.manifest.add(added, true);
    }

    public void removeSSTable(SSTableReader sstable) {
        this.manifest.remove(sstable);
    }

    protected Set<SSTableReader> getSSTables() {
        return this.manifest.getSSTables();
    }

    public String toString() {
        return String.format("LCS@%d(%s)", new Object[]{Integer.valueOf(this.hashCode()), this.cfs.name});
    }

    private SSTableReader findDroppableSSTable(final int gcBefore) {
        for (int i = this.manifest.getLevelCount(); i >= 0; --i) {
            SortedSet<SSTableReader> sstables = this.manifest.getLevelSorted(i, new Comparator<SSTableReader>() {
                public int compare(SSTableReader o1, SSTableReader o2) {
                    double r1 = o1.getEstimatedDroppableTombstoneRatio(gcBefore);
                    double r2 = o2.getEstimatedDroppableTombstoneRatio(gcBefore);
                    return -1 * Doubles.compare(r1, r2);
                }
            });
            if (!sstables.isEmpty()) {
                Set<SSTableReader> compacting = this.cfs.getTracker().getCompacting();
                Iterator var5 = sstables.iterator();

                while (var5.hasNext()) {
                    SSTableReader sstable = (SSTableReader) var5.next();
                    if (sstable.getEstimatedDroppableTombstoneRatio(gcBefore) <= (double) this.tombstoneThreshold) {
                        break;
                    }

                    if (!compacting.contains(sstable) && !sstable.isMarkedSuspect() && this.worthDroppingTombstones(sstable, gcBefore)) {
                        return sstable;
                    }
                }
            }
        }

        return null;
    }

    public CompactionLogger.Strategy strategyLogger() {
        return new CompactionLogger.Strategy() {
            public JsonNode sstable(SSTableReader sstable) {
                ObjectNode node = JsonNodeFactory.instance.objectNode();
                node.put("level", sstable.getSSTableLevel());
                node.put("min_token", sstable.first.getToken().toString());
                node.put("max_token", sstable.last.getToken().toString());
                return node;
            }

            public JsonNode options() {
                return null;
            }
        };
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException {
        Map<String, String> uncheckedOptions = AbstractCompactionStrategy.validateOptions(options);
        String size = options.containsKey("sstable_size_in_mb") ? (String) options.get("sstable_size_in_mb") : "1";

        try {
            int ssSize = Integer.parseInt(size);
            if (ssSize < 1) {
                throw new ConfigurationException(String.format("%s must be larger than 0, but was %s", new Object[]{"sstable_size_in_mb", Integer.valueOf(ssSize)}));
            }
        } catch (NumberFormatException var6) {
            throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", new Object[]{size, "sstable_size_in_mb"}), var6);
        }

        uncheckedOptions.remove("sstable_size_in_mb");
        String levelFanoutSize = options.containsKey("fanout_size") ? (String) options.get("fanout_size") : String.valueOf(10);

        try {
            int fanoutSize = Integer.parseInt(levelFanoutSize);
            if (fanoutSize < 1) {
                throw new ConfigurationException(String.format("%s must be larger than 0, but was %s", new Object[]{"fanout_size", Integer.valueOf(fanoutSize)}));
            }
        } catch (NumberFormatException var5) {
            throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", new Object[]{size, "fanout_size"}), var5);
        }

        uncheckedOptions.remove("fanout_size");
        uncheckedOptions = SizeTieredCompactionStrategyOptions.validateOptions(options, uncheckedOptions);
        return uncheckedOptions;
    }

    private static class LeveledScanner extends AbstractIterator<UnfilteredRowIterator> implements ISSTableScanner {
        private final TableMetadata metadata;
        private final Collection<Range<Token>> ranges;
        private final List<SSTableReader> sstables;
        private final Iterator<SSTableReader> sstableIterator;
        private final long totalLength;
        private final long compressedLength;
        private ISSTableScanner currentScanner;
        private long positionOffset;
        private long totalBytesScanned = 0L;

        public LeveledScanner(TableMetadata metadata, Collection<SSTableReader> sstables, Collection<Range<Token>> ranges) {
            this.metadata = metadata;
            this.ranges = ranges;
            this.sstables = new ArrayList(sstables.size());
            long length = 0L;
            long cLength = 0L;

            SSTableReader sstable;
            double estKeysInRangeRatio;
            for (Iterator var8 = sstables.iterator(); var8.hasNext(); cLength = (long) ((double) cLength + (double) sstable.onDiskLength() * estKeysInRangeRatio)) {
                sstable = (SSTableReader) var8.next();
                this.sstables.add(sstable);
                long estimatedKeys = sstable.estimatedKeys();
                estKeysInRangeRatio = 1.0D;
                if (estimatedKeys > 0L && ranges != null) {
                    estKeysInRangeRatio = (double) sstable.estimatedKeysForRanges(ranges) / (double) estimatedKeys;
                }

                length = (long) ((double) length + (double) sstable.uncompressedLength() * estKeysInRangeRatio);
            }

            this.totalLength = length;
            this.compressedLength = cLength;
            Collections.sort(this.sstables, SSTableReader.sstableComparator);
            this.sstableIterator = this.sstables.iterator();

            assert this.sstableIterator.hasNext();

            SSTableReader currentSSTable = (SSTableReader) this.sstableIterator.next();
            this.currentScanner = currentSSTable.getScanner(ranges);
        }

        public static Collection<SSTableReader> intersecting(Collection<SSTableReader> sstables, Collection<Range<Token>> ranges) {
            if (ranges == null) {
                return Lists.newArrayList(sstables);
            } else {
                Set<SSTableReader> filtered = new HashSet();
                Iterator var3 = ranges.iterator();

                label31:
                while (var3.hasNext()) {
                    Range<Token> range = (Range) var3.next();
                    Iterator var5 = sstables.iterator();

                    while (true) {
                        SSTableReader sstable;
                        Range sstableRange;
                        do {
                            if (!var5.hasNext()) {
                                continue label31;
                            }

                            sstable = (SSTableReader) var5.next();
                            sstableRange = new Range(sstable.first.getToken(), sstable.last.getToken());
                        } while (range != null && !sstableRange.intersects(range));

                        filtered.add(sstable);
                    }
                }

                return filtered;
            }
        }

        public TableMetadata metadata() {
            return this.metadata;
        }

        protected UnfilteredRowIterator computeNext() {
            if (this.currentScanner == null) {
                return (UnfilteredRowIterator) this.endOfData();
            } else {
                while (!this.currentScanner.hasNext()) {
                    this.positionOffset += this.currentScanner.getLengthInBytes();
                    this.totalBytesScanned += this.currentScanner.getBytesScanned();
                    this.currentScanner.close();
                    if (!this.sstableIterator.hasNext()) {
                        this.currentScanner = null;
                        return (UnfilteredRowIterator) this.endOfData();
                    }

                    SSTableReader currentSSTable = (SSTableReader) this.sstableIterator.next();
                    this.currentScanner = currentSSTable.getScanner(this.ranges);
                }

                return (UnfilteredRowIterator) this.currentScanner.next();
            }
        }

        public void close() {
            if (this.currentScanner != null) {
                this.currentScanner.close();
            }

        }

        public long getLengthInBytes() {
            return this.totalLength;
        }

        public long getCurrentPosition() {
            return this.positionOffset + (this.currentScanner == null ? 0L : this.currentScanner.getCurrentPosition());
        }

        public long getCompressedLengthInBytes() {
            return this.compressedLength;
        }

        public long getBytesScanned() {
            return this.currentScanner == null ? this.totalBytesScanned : this.totalBytesScanned + this.currentScanner.getBytesScanned();
        }

        public String getBackingFiles() {
            return Joiner.on(", ").join(this.sstables);
        }
    }
}
