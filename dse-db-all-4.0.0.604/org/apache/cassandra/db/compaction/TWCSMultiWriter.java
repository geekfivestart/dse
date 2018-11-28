package org.apache.cassandra.db.compaction;

import com.google.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.SimpleSSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;

public class TWCSMultiWriter implements SSTableMultiWriter {
    private static final int MAX_BUCKETS = 10;
    private static final int ALL_BUCKETS = 12;
    private static final int OLD_BUCKET_INDEX = 10;
    private static final int FUTURE_BUCKET_INDEX = 11;
    private final ColumnFamilyStore cfs;
    private final TimeUnit windowTimeUnit;
    private final int windowTimeSize;
    private final TimeUnit timestampResolution;
    private final Descriptor descriptor;
    private final long keyCount;
    private final long repairedAt;
    private final UUID pendingRepair;
    private final MetadataCollector meta;
    private final SerializationHeader header;
    private final Collection<Index> indexes;
    private final LifecycleTransaction txn;
    private final TWCSMultiWriter.BucketIndexer bucketIndex;
    private final SSTableMultiWriter[] writers = new SSTableMultiWriter[12];

    public TWCSMultiWriter(ColumnFamilyStore cfs, TimeUnit windowTimeUnit, int windowTimeSize, TimeUnit timestampResolution, Descriptor descriptor, long keyCount, long repairedAt, UUID pendingRepair, MetadataCollector meta, SerializationHeader header, Collection<Index> indexes, LifecycleTransaction txn) {
        this.cfs = cfs;
        this.windowTimeUnit = windowTimeUnit;
        this.windowTimeSize = windowTimeSize;
        this.timestampResolution = timestampResolution;
        this.descriptor = descriptor;
        this.keyCount = keyCount;
        this.repairedAt = repairedAt;
        this.pendingRepair = pendingRepair;
        this.meta = meta;
        this.header = header;
        this.indexes = indexes;
        this.txn = txn;
        this.bucketIndex = createBucketIndexes(windowTimeUnit, windowTimeSize);
    }

    public static TWCSMultiWriter.BucketIndexer createBucketIndexes(TimeUnit windowTimeUnit, int windowTimeSize) {
        long tmpMaxBucket = 0L;
        long tmpMinBucket = 9223372036854775807L;
        long now = System.currentTimeMillis();
        long[] buckets = new long[10];

        for (int i = 0; i < buckets.length; ++i) {
            long bucket = ((Long) TimeWindowCompactionStrategy.getWindowBoundsInMillis(windowTimeUnit, windowTimeSize, now - TimeUnit.MILLISECONDS.convert((long) (windowTimeSize * i), windowTimeUnit)).left).longValue();
            buckets[buckets.length - i - 1] = bucket;
            tmpMaxBucket = Math.max(tmpMaxBucket, bucket);
            tmpMinBucket = Math.min(tmpMinBucket, bucket);
        }

        return new TWCSMultiWriter.BucketIndexer(buckets, tmpMinBucket, tmpMaxBucket);
    }

    public boolean append(UnfilteredRowIterator partition) {
        boolean ret;
        int i;
        UnfilteredRowIterator[] iterators = TWCSMultiWriter.splitPartitionOnTime(partition, this.bucketIndex, new TWCSConfig(this.windowTimeUnit, this.windowTimeSize, this.timestampResolution));
        ret = false;
        try {
            for (i = 0; i < iterators.length; ++i) {
                UnfilteredRowIterator iterator = iterators[i];
                if (iterator == null) continue;
                SSTableMultiWriter writer = this.writers[i];
                if (writer == null) {
                    Descriptor newDesc = this.cfs.newSSTableDescriptor(this.descriptor.directory);
                    long minTimestamp = this.header.stats().minTimestamp;
                    if (i != 10) {
                        minTimestamp = this.bucketIndex.getTimestampForIndex(i);
                    }
                    this.writers[i] = writer = SimpleSSTableMultiWriter.create(newDesc, this.keyCount, this.repairedAt, this.pendingRepair, this.cfs.metadata, this.meta.copy(), new SerializationHeader(this.header.isForSSTable(), this.cfs.metadata(), this.header.columns(), new EncodingStats(minTimestamp, this.header.stats().minLocalDeletionTime, this.header.stats().minTTL)), this.indexes, this.txn);
                }
                ret |= writer.append(iterator);
            }
        } catch (Throwable throwable) {
            for (int i2 = 0; i2 < iterators.length; ++i2) {
                if (iterators[i2] != null) {
                    iterators[i2].close();
                }
                iterators[i2] = null;
            }
            throw throwable;
        }
        for (i = 0; i < iterators.length; ++i) {
            if (iterators[i] != null) {
                iterators[i].close();
            }
            iterators[i] = null;
        }
        return ret;
    }

    public Collection<SSTableReader> finish(long repairedAt, long maxDataAge, boolean openResult) {
        List<SSTableReader> sstables = new ArrayList(this.writers.length);
        SSTableMultiWriter[] var7 = this.writers;
        int var8 = var7.length;

        for (int var9 = 0; var9 < var8; ++var9) {
            SSTableMultiWriter writer = var7[var9];
            if (writer != null) {
                sstables.addAll(writer.finish(repairedAt, maxDataAge, openResult));
            }
        }

        return sstables;
    }

    public Collection<SSTableReader> finish(boolean openResult) {
        List<SSTableReader> sstables = new ArrayList(this.writers.length);
        SSTableMultiWriter[] var3 = this.writers;
        int var4 = var3.length;

        for (int var5 = 0; var5 < var4; ++var5) {
            SSTableMultiWriter writer = var3[var5];
            if (writer != null) {
                sstables.addAll(writer.finish(openResult));
            }
        }

        return sstables;
    }

    public Collection<SSTableReader> finished() {
        List<SSTableReader> sstables = new ArrayList(this.writers.length);
        SSTableMultiWriter[] var2 = this.writers;
        int var3 = var2.length;

        for (int var4 = 0; var4 < var3; ++var4) {
            SSTableMultiWriter writer = var2[var4];
            if (writer != null) {
                sstables.addAll(writer.finished());
            }
        }

        return sstables;
    }

    public SSTableMultiWriter setOpenResult(boolean openResult) {
        SSTableMultiWriter[] var2 = this.writers;
        int var3 = var2.length;

        for (int var4 = 0; var4 < var3; ++var4) {
            SSTableMultiWriter writer = var2[var4];
            if (writer != null) {
                writer.setOpenResult(openResult);
            }
        }

        return this;
    }

    public String getFilename() {
        SSTableMultiWriter[] var1 = this.writers;
        int var2 = var1.length;

        for (int var3 = 0; var3 < var2; ++var3) {
            SSTableMultiWriter writer = var1[var3];
            if (writer != null) {
                return writer.getFilename();
            }
        }

        return "";
    }

    public long getFilePointer() {
        long filepointerSum = 0L;
        SSTableMultiWriter[] var3 = this.writers;
        int var4 = var3.length;

        for (int var5 = 0; var5 < var4; ++var5) {
            SSTableMultiWriter writer = var3[var5];
            if (writer != null) {
                filepointerSum += writer.getFilePointer();
            }
        }

        return filepointerSum;
    }

    public TableId getTableId() {
        return this.cfs.metadata.id;
    }

    public Throwable commit(Throwable accumulate) {
        Throwable t = accumulate;
        SSTableMultiWriter[] var3 = this.writers;
        int var4 = var3.length;

        for (int var5 = 0; var5 < var4; ++var5) {
            SSTableMultiWriter writer = var3[var5];
            if (writer != null) {
                t = writer.commit(t);
            }
        }

        return t;
    }

    public Throwable abort(Throwable accumulate) {
        Throwable t = accumulate;
        SSTableMultiWriter[] var3 = this.writers;
        int var4 = var3.length;

        for (int var5 = 0; var5 < var4; ++var5) {
            SSTableMultiWriter writer = var3[var5];
            if (writer != null) {
                t = writer.abort(t);
            }
        }

        return t;
    }

    public void prepareToCommit() {
        SSTableMultiWriter[] var1 = this.writers;
        int var2 = var1.length;

        for (int var3 = 0; var3 < var2; ++var3) {
            SSTableMultiWriter writer = var1[var3];
            if (writer != null) {
                writer.prepareToCommit();
            }
        }

    }

    public void close() {
        SSTableMultiWriter[] var1 = this.writers;
        int var2 = var1.length;

        for (int var3 = 0; var3 < var2; ++var3) {
            SSTableMultiWriter writer = var1[var3];
            if (writer != null) {
                writer.close();
            }
        }

    }

    @VisibleForTesting
    public static UnfilteredRowIterator[] splitPartitionOnTime(UnfilteredRowIterator partition, TWCSMultiWriter.BucketIndexer bucketIndex, TWCSMultiWriter.TWCSConfig config) {
        List<Unfiltered>[] rows = new List[12];
        Unfiltered[] staticRows = splitRow(partition.staticRow(), bucketIndex, config);

        while (true) {
            while (partition.hasNext()) {
                Unfiltered unfiltered = (Unfiltered) partition.next();
                if (unfiltered.kind() == Unfiltered.Kind.ROW) {
                    Row r = (Row) unfiltered;
                    Unfiltered[] splitRows = splitRow(r, bucketIndex, config);

                    for (int i = 0; i < splitRows.length; ++i) {
                        if (splitRows[i] != null) {
                            if (rows[i] == null) {
                                rows[i] = new ArrayList();
                            }

                            rows[i].add(splitRows[i]);
                        }
                    }
                } else {
                    RangeTombstoneMarker marker = (RangeTombstoneMarker) unfiltered;
                    boolean isReversed = partition.isReverseOrder();
                    long opentwcsBucket = ((Long) TimeWindowCompactionStrategy.getWindowBoundsInMillis(config.windowTimeUnit, config.windowTimeSize, toMillis(marker.openDeletionTime(isReversed).markedForDeleteAt(), config.timestampResolution)).left).longValue();
                    long closetwcsBucket = ((Long) TimeWindowCompactionStrategy.getWindowBoundsInMillis(config.windowTimeUnit, config.windowTimeSize, toMillis(marker.closeDeletionTime(isReversed).markedForDeleteAt(), config.timestampResolution)).left).longValue();
                    int idx;
                    if (opentwcsBucket == closetwcsBucket) {
                        idx = bucketIndex.get(opentwcsBucket);
                        if (rows[idx] == null) {
                            rows[idx] = new ArrayList();
                        }

                        rows[idx].add(marker);
                    } else {
                        idx = bucketIndex.get(opentwcsBucket);
                        if (rows[idx] == null) {
                            rows[idx] = new ArrayList();
                        }

                        rows[idx].add(new RangeTombstoneBoundMarker(marker.openBound(isReversed), marker.openDeletionTime(isReversed)));
                        idx = bucketIndex.get(closetwcsBucket);
                        if (rows[idx] == null) {
                            rows[idx] = new ArrayList();
                        }

                        rows[idx].add(new RangeTombstoneBoundMarker(marker.closeBound(isReversed), marker.closeDeletionTime(isReversed)));
                    }
                }
            }

            UnfilteredRowIterator[] ret = new UnfilteredRowIterator[12];
            int partitionDeletionBucket = -1;
            if (!partition.partitionLevelDeletion().isLive()) {
                partitionDeletionBucket = bucketIndex.get(((Long) TimeWindowCompactionStrategy.getWindowBoundsInMillis(config.windowTimeUnit, config.windowTimeSize, toMillis(partition.partitionLevelDeletion().markedForDeleteAt(), config.timestampResolution)).left).longValue());
            }

            for (int i = 0; i < rows.length; ++i) {
                Row staticRow = Rows.EMPTY_STATIC_ROW;
                if (staticRows[i] != null) {
                    staticRow = (Row) staticRows[i];
                }

                DeletionTime partitionLevelDeletion = DeletionTime.LIVE;
                if (partitionDeletionBucket == i) {
                    partitionLevelDeletion = partition.partitionLevelDeletion();
                }

                List<Unfiltered> row = rows[i];
                if (row == null) {
                    if (staticRow != Rows.EMPTY_STATIC_ROW || partitionLevelDeletion != DeletionTime.LIVE) {
                        ret[i] = new TWCSMultiWriter.UnfilteredRowIteratorBucket(Collections.emptyList(), partition.metadata(), partition.partitionKey(), partitionLevelDeletion, partition.columns(), staticRow, partition.isReverseOrder(), partition.stats());
                    }
                } else {
                    ret[i] = new TWCSMultiWriter.UnfilteredRowIteratorBucket(row, partition.metadata(), partition.partitionKey(), partitionLevelDeletion, partition.columns(), staticRow, partition.isReverseOrder(), partition.stats());
                }
            }

            return ret;
        }
    }

    private static Unfiltered[] splitRow(Row r, TWCSMultiWriter.BucketIndexer bucketIndex, TWCSMultiWriter.TWCSConfig config) {
        Row.Builder[] rowBuilders = new Row.Builder[12];
        boolean hasBuilder = false;
        long[] maxTimestampPerRow = new long[12];
        Row.Builder rowBuilder;
        if (!r.deletion().isLive()) {
            long ts = r.deletion().time().markedForDeleteAt();
            rowBuilder = getBuilder(ts, rowBuilders, bucketIndex, r.clustering(), maxTimestampPerRow, config);
            rowBuilder.addRowDeletion(r.deletion());
            hasBuilder = true;
        }

        for (Iterator var10 = r.cells().iterator(); var10.hasNext(); hasBuilder = true) {
            Cell c = (Cell) var10.next();
            if (r.hasComplexDeletion() && !r.getComplexColumnData(c.column()).complexDeletion().isLive()) {
                DeletionTime dt = r.getComplexColumnData(c.column()).complexDeletion();
                Row.Builder builder = getBuilder(dt.markedForDeleteAt(), rowBuilders, bucketIndex, r.clustering(), maxTimestampPerRow, config);
                builder.addComplexDeletion(c.column(), dt);
            }

            rowBuilder = getBuilder(c.timestamp(), rowBuilders, bucketIndex, r.clustering(), maxTimestampPerRow, config);
            rowBuilder.addCell(c);
        }

        if (!hasBuilder && !r.primaryKeyLivenessInfo().isEmpty()) {
            getBuilder(r.primaryKeyLivenessInfo().timestamp(), rowBuilders, bucketIndex, r.clustering(), maxTimestampPerRow, config);
        }

        Unfiltered[] splitRows = new Unfiltered[rowBuilders.length];

        for (int i = 0; i < rowBuilders.length; ++i) {
            rowBuilder = rowBuilders[i];
            if (rowBuilder != null) {
                if (!r.primaryKeyLivenessInfo().isEmpty()) {
                    LivenessInfo li = r.primaryKeyLivenessInfo().withUpdatedTimestamp(maxTimestampPerRow[i]);
                    rowBuilder.addPrimaryKeyLivenessInfo(li);
                }

                splitRows[i] = rowBuilder.build();
            }
        }

        return splitRows;
    }

    private static long toMillis(long timestamp, TimeUnit timestampResolution) {
        return TimeUnit.MILLISECONDS.convert(timestamp, timestampResolution);
    }

    private static Row.Builder getBuilder(long timestamp, Row.Builder[] rowBuilders, TWCSMultiWriter.BucketIndexer indexes, Clustering c, long[] maxTimestampPerRow, TWCSMultiWriter.TWCSConfig config) {
        long twcsBucket = ((Long) TimeWindowCompactionStrategy.getWindowBoundsInMillis(config.windowTimeUnit, config.windowTimeSize, toMillis(timestamp, config.timestampResolution)).left).longValue();
        int idx = indexes.get(twcsBucket);
        maxTimestampPerRow[idx] = Math.max(timestamp, maxTimestampPerRow[idx]);
        Row.Builder builder = rowBuilders[idx];
        if (builder == null) {
            builder = Row.Builder.sorted();
            rowBuilders[idx] = builder;
            builder.newRow(c);
        }

        return builder;
    }

    @VisibleForTesting
    public static class TWCSConfig {
        private final TimeUnit windowTimeUnit;
        private final int windowTimeSize;
        private final TimeUnit timestampResolution;

        public TWCSConfig(TimeUnit windowTimeUnit, int windowTimeSize, TimeUnit timestampResolution) {
            this.windowTimeUnit = windowTimeUnit;
            this.windowTimeSize = windowTimeSize;
            this.timestampResolution = timestampResolution;
        }
    }

    public static class BucketIndexer {
        @VisibleForTesting
        public final long[] buckets;
        private final long minBucket;
        private final long maxBucket;

        public BucketIndexer(long[] buckets, long minBucket, long maxBucket) {
            this.buckets = buckets;
            this.minBucket = minBucket;
            this.maxBucket = maxBucket;
        }

        public int get(long twcsBucket) {
            if (twcsBucket < this.minBucket) {
                return 10;
            } else if (twcsBucket > this.maxBucket) {
                return 11;
            } else {
                int index = Arrays.binarySearch(this.buckets, twcsBucket);

                assert index >= 0;

                return index;
            }
        }

        long getTimestampForIndex(int bucket) {
            return bucket == 10 ? -9223372036854775808L : (bucket == 11 ? this.maxBucket : this.buckets[bucket]);
        }
    }

    private static class UnfilteredRowIteratorBucket extends AbstractUnfilteredRowIterator {
        private final Iterator<Unfiltered> unfiltereds;

        UnfilteredRowIteratorBucket(List<Unfiltered> uris, TableMetadata metadata, DecoratedKey partitionKey, DeletionTime partitionLevelDeletion, RegularAndStaticColumns columns, Row staticRow, boolean isReverseOrder, EncodingStats stats) {
            super(metadata, partitionKey, partitionLevelDeletion, columns, staticRow, isReverseOrder, stats);
            this.unfiltereds = uris.iterator();
        }

        protected Unfiltered computeNext() {
            return !this.unfiltereds.hasNext() ? (Unfiltered) this.endOfData() : (Unfiltered) this.unfiltereds.next();
        }
    }
}
