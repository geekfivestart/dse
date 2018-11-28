package org.apache.cassandra.io.sstable.format;

import io.reactivex.functions.Function;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.FileAccessType;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.flow.FlowSource;
import org.apache.cassandra.utils.flow.FlowSubscriber;
import org.apache.cassandra.utils.flow.FlowSubscription;
import org.apache.cassandra.utils.flow.FlowSubscriptionRecipient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncSSTableScanner extends FlowSource<FlowableUnfilteredPartition> implements FlowSubscriptionRecipient {
   private static final Logger logger = LoggerFactory.getLogger(AsyncSSTableScanner.class);
   private final SSTableReader sstable;
   private final RandomAccessReader dfile;
   private final List<AbstractBounds<PartitionPosition>> ranges;
   private final ColumnFilter columns;
   private final DataRange dataRange;
   private final SSTableReadsListener listener;
   private final Flow<FlowableUnfilteredPartition> sourceFlow;
   private FlowSubscription source;

   private AsyncSSTableScanner(SSTableReader sstable, ColumnFilter columns, DataRange dataRange, List<AbstractBounds<PartitionPosition>> ranges, SSTableReadsListener listener) {
      assert sstable != null;

      this.sstable = sstable;
      this.dfile = sstable.openDataReader(Rebufferer.ReaderConstraint.ASYNC, FileAccessType.SEQUENTIAL);
      this.columns = columns;
      this.dataRange = dataRange;
      this.ranges = ranges;
      this.listener = listener;
      this.sourceFlow = this.flow();
      if(logger.isTraceEnabled()) {
         logger.trace("Scanning {} with {}", this.dfile.getPath(), this.dfile);
      }

   }

   public static AsyncSSTableScanner getScanner(SSTableReader sstable) {
      return getScanner(sstable, Collections.singletonList(SSTableScanner.fullRange(sstable)));
   }

   public static AsyncSSTableScanner getScanner(SSTableReader sstable, Collection<Range<Token>> ranges) {
      return getScanner(sstable, SSTableScanner.makeBounds(sstable, ranges));
   }

   public static AsyncSSTableScanner getScanner(SSTableReader sstable, List<AbstractBounds<PartitionPosition>> bounds) {
      return new AsyncSSTableScanner(sstable, ColumnFilter.all(sstable.metadata()), (DataRange)null, bounds, SSTableReadsListener.NOOP_LISTENER);
   }

   public static AsyncSSTableScanner getScanner(SSTableReader sstable, ColumnFilter columns, DataRange dataRange, SSTableReadsListener listener) {
      return new AsyncSSTableScanner(sstable, columns, dataRange, SSTableScanner.makeBounds(sstable, dataRange), listener);
   }

   private Flow<FlowableUnfilteredPartition> flow() {
      return this.ranges.size() == 1?this.sstable.coveredKeysFlow(this.dfile, this.ranges.get(0)).flatMap(this::partitions):Flow.fromIterable(this.ranges).flatMap((range) -> {
         return this.sstable.coveredKeysFlow(this.dfile, range);
      }).flatMap(this::partitions);
   }

   private Flow<FlowableUnfilteredPartition> partitions(IndexFileEntry entry) {
      if(this.dataRange == null) {
         return this.sstable.flow(entry, this.dfile, this.listener);
      } else {
         ClusteringIndexFilter filter = this.dataRange.clusteringIndexFilter(entry.key);
         return this.sstable.flow(entry, this.dfile, filter.getSlices(this.sstable.metadata()), this.columns, filter.isReversed(), this.listener);
      }
   }

   public void onSubscribe(FlowSubscription source) {
      this.source = source;
   }

   public void requestFirst(FlowSubscriber<FlowableUnfilteredPartition> subscriber, FlowSubscriptionRecipient subscriptionRecipient) {
      this.listener.onScanningStarted(this.sstable);
      this.subscribe(subscriber, subscriptionRecipient);
      this.sourceFlow.requestFirst(subscriber, this);
   }

   public void requestNext() {
      this.source.requestNext();
   }

   public void close() throws Exception {
      Throwables.maybeFail(Throwables.closeNonNull((Throwable)null, (AutoCloseable[])(new AutoCloseable[]{this.dfile, this.source})));
   }

   public String toString() {
      return Flow.formatTrace(this.getClass().getSimpleName(), (Object)this.source);
   }
}
