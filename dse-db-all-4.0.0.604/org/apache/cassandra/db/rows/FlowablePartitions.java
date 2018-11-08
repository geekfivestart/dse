package org.apache.cassandra.db.rows;

import com.google.common.base.Throwables;
import io.reactivex.functions.Action;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.db.Clusterable;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.Reducer;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.flow.FlowSubscriber;
import org.apache.cassandra.utils.flow.FlowSubscriptionRecipient;
import org.apache.cassandra.utils.flow.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlowablePartitions {
   private static final Logger logger = LoggerFactory.getLogger(FlowablePartitions.class);
   private static final Comparator<FlowableUnfilteredPartition> flowablePartitionComparator = Comparator.comparing((x) -> {
      return x.header().partitionKey;
   });

   public FlowablePartitions() {
   }

   public static UnfilteredRowIterator toIterator(final FlowableUnfilteredPartition partition) {
      try {
         final CloseableIterator<Unfiltered> iterator = Flow.toIterator(partition.content());
         class URI extends FlowablePartitions.BaseRowIterator<Unfiltered> implements UnfilteredRowIterator {
            URI() {
               super(partition, iterator);
            }
         }

         return new URI();
      } catch (Exception var2) {
         throw Throwables.propagate(var2);
      }
   }

   public static RowIterator toIteratorFiltered(final FlowablePartition partition) {
      try {
         final CloseableIterator<Row> iterator = Flow.toIterator(partition.content());
         class RI extends FlowablePartitions.BaseRowIterator<Row> implements RowIterator {
            RI() {
               super(partition, iterator);
            }
         }

         return new RI();
      } catch (Exception var2) {
         throw Throwables.propagate(var2);
      }
   }

   public static FlowableUnfilteredPartition fromIterator(UnfilteredRowIterator iter) {
      return new FlowablePartitions.FromUnfilteredRowIterator(iter);
   }

   public static FlowablePartition fromIterator(RowIterator iter, StagedScheduler callOn) {
      return new FlowablePartitions.FromRowIterator(iter, callOn);
   }

   public static FlowableUnfilteredPartition empty(TableMetadata metadata, DecoratedKey partitionKey, boolean reversed) {
      return FlowableUnfilteredPartition.create(PartitionHeader.empty(metadata, partitionKey, reversed), Rows.EMPTY_STATIC_ROW, Flow.empty());
   }

   public static FlowableUnfilteredPartition merge(List<FlowableUnfilteredPartition> flowables, int nowInSec, UnfilteredRowIterators.MergeListener listener) {
      assert !flowables.isEmpty();

      FlowableUnfilteredPartition first = (FlowableUnfilteredPartition)flowables.get(0);
      if(flowables.size() == 1 && listener == null) {
         return first.skipLowerBound();
      } else {
         List<PartitionHeader> headers = new ArrayList(flowables.size());
         List<Flow<Unfiltered>> contents = new ArrayList(flowables.size());
         Iterator var6 = flowables.iterator();

         while(var6.hasNext()) {
            FlowableUnfilteredPartition flowable = (FlowableUnfilteredPartition)var6.next();
            headers.add(flowable.header());
            contents.add(flowable.content());
         }

         PartitionHeader header = PartitionHeader.merge(headers, listener);
         MergeReducer reducer = new MergeReducer(flowables.size(), nowInSec, header, listener);
         Row staticRow;
         if(!header.columns.statics.isEmpty()) {
            staticRow = mergeStaticRows(flowables, header, nowInSec, listener);
         } else {
            staticRow = Rows.EMPTY_STATIC_ROW;
         }

         Comparator<Clusterable> comparator = header.metadata.comparator;
         if(header.isReverseOrder) {
            comparator = ((Comparator)comparator).reversed();
         }

         Flow<Unfiltered> content = Flow.merge(contents, (Comparator)comparator, reducer);
         if(listener != null) {
            listener.getClass();
            content = content.doOnClose(listener::close);
         }

         return FlowableUnfilteredPartition.create(header, staticRow, content);
      }
   }

   public static Row mergeStaticRows(List<FlowableUnfilteredPartition> sources, PartitionHeader header, int nowInSec, UnfilteredRowIterators.MergeListener listener) {
      Columns columns = header.columns.statics;
      if(columns.isEmpty()) {
         return Rows.EMPTY_STATIC_ROW;
      } else {
         boolean hasStatic = false;

         FlowableUnfilteredPartition source;
         for(Iterator var6 = sources.iterator(); var6.hasNext(); hasStatic |= !source.staticRow().isEmpty()) {
            source = (FlowableUnfilteredPartition)var6.next();
         }

         if(!hasStatic) {
            return Rows.EMPTY_STATIC_ROW;
         } else {
            Row.Merger merger = new Row.Merger(sources.size(), nowInSec, columns.size(), columns.hasComplex());

            for(int i = 0; i < sources.size(); ++i) {
               merger.add(i, ((FlowableUnfilteredPartition)sources.get(i)).staticRow());
            }

            Row merged = merger.merge(header.partitionLevelDeletion);
            if(merged == null) {
               merged = Rows.EMPTY_STATIC_ROW;
            }

            if(listener != null) {
               listener.onMergedRows(merged, merger.mergedRows());
            }

            return merged;
         }
      }
   }

   public static Flow<FlowableUnfilteredPartition> mergePartitions(final List<Flow<FlowableUnfilteredPartition>> sources, final int nowInSec, final FlowablePartitions.MergeListener listener) {
      assert !sources.isEmpty();

      if(sources.size() == 1 && listener == null) {
         return (Flow)sources.get(0);
      } else {
         Flow<FlowableUnfilteredPartition> merge = Flow.merge(sources, flowablePartitionComparator, new Reducer<FlowableUnfilteredPartition, FlowableUnfilteredPartition>() {
            private final FlowableUnfilteredPartition[] toMerge = new FlowableUnfilteredPartition[sources.size()];
            private PartitionHeader header;

            public void reduce(int idx, FlowableUnfilteredPartition current) {
               this.header = current.header();
               this.toMerge[idx] = current;
            }

            public FlowableUnfilteredPartition getReduced() {
               UnfilteredRowIterators.MergeListener rowListener = listener == null?null:listener.getRowMergeListener(this.header.partitionKey, this.toMerge);
               return rowListener == null?this.mergeNonEmptyPartitions():this.mergeAllPartitions(rowListener);
            }

            private FlowableUnfilteredPartition mergeAllPartitions(UnfilteredRowIterators.MergeListener rowListener) {
               FlowableUnfilteredPartition empty = null;
               int i = 0;

               for(int length = this.toMerge.length; i < length; ++i) {
                  FlowableUnfilteredPartition element = this.toMerge[i];
                  if(element == null) {
                     if(empty == null) {
                        empty = FlowablePartitions.empty(this.header.metadata, this.header.partitionKey, this.header.isReverseOrder);
                     }

                     this.toMerge[i] = empty;
                  }
               }

               return FlowablePartitions.merge(Arrays.asList(this.toMerge), nowInSec, rowListener);
            }

            private FlowableUnfilteredPartition mergeNonEmptyPartitions() {
               List<FlowableUnfilteredPartition> nonEmptyPartitions = new ArrayList(this.toMerge.length);
               int i = 0;

               for(int length = this.toMerge.length; i < length; ++i) {
                  FlowableUnfilteredPartition element = this.toMerge[i];
                  if(element != null) {
                     nonEmptyPartitions.add(element);
                  }
               }

               if(nonEmptyPartitions.isEmpty()) {
                  return FlowablePartitions.empty(this.header.metadata, this.header.partitionKey, this.header.isReverseOrder);
               } else if(nonEmptyPartitions.size() == 1) {
                  return (FlowableUnfilteredPartition)nonEmptyPartitions.get(0);
               } else {
                  return FlowablePartitions.merge(nonEmptyPartitions, nowInSec, (UnfilteredRowIterators.MergeListener)null);
               }
            }

            public void onKeyChange() {
               Arrays.fill(this.toMerge, (Object)null);
            }

            public boolean trivialReduceIsTrivial() {
               return listener == null;
            }
         });
         if(listener != null) {
            listener.getClass();
            merge = merge.doOnClose(listener::close);
         }

         return merge;
      }
   }

   public static Flow<FlowableUnfilteredPartition> fromPartitions(UnfilteredPartitionIterator iter) {
      return Flow.fromIterable(() -> {
         return iter;
      }).map((i) -> {
         return fromIterator(i);
      });
   }

   public static Flow<FlowablePartition> fromPartitions(PartitionIterator iter, StagedScheduler scheduler) {
      Flow<FlowablePartition> flow = Flow.fromIterable(() -> {
         return iter;
      }).map((i) -> {
         return fromIterator(i, scheduler);
      });
      if(scheduler != null) {
         flow = flow.lift(Threads.requestOn(scheduler, TPCTaskType.READ_FROM_ITERATOR));
      }

      return flow;
   }

   public static UnfilteredPartitionIterator toPartitions(Flow<FlowableUnfilteredPartition> partitions, final TableMetadata metadata) {
      try {
         final CloseableIterator<FlowableUnfilteredPartition> iterator = Flow.toIterator(partitions);
         return new UnfilteredPartitionIterator() {
            public TableMetadata metadata() {
               return metadata;
            }

            public void close() {
               iterator.close();
            }

            public boolean hasNext() {
               return iterator.hasNext();
            }

            public UnfilteredRowIterator next() {
               return FlowablePartitions.toIterator((FlowableUnfilteredPartition)iterator.next());
            }
         };
      } catch (Exception var3) {
         throw Throwables.propagate(var3);
      }
   }

   public static PartitionIterator toPartitionsFiltered(Flow<FlowablePartition> partitions) {
      try {
         final CloseableIterator<FlowablePartition> iterator = Flow.toIterator(partitions);
         return new PartitionIterator() {
            public void close() {
               iterator.close();
            }

            public boolean hasNext() {
               return iterator.hasNext();
            }

            public RowIterator next() {
               return FlowablePartitions.toIteratorFiltered((FlowablePartition)iterator.next());
            }
         };
      } catch (Exception var2) {
         throw Throwables.propagate(var2);
      }
   }

   private static Row filterStaticRow(Row row, int nowInSec, boolean enforceStrictLiveness) {
      if(row != null && !row.isEmpty()) {
         row = row.purge(DeletionPurger.PURGE_ALL, nowInSec, enforceStrictLiveness);
         return row == null?Rows.EMPTY_STATIC_ROW:row;
      } else {
         return Rows.EMPTY_STATIC_ROW;
      }
   }

   public static FlowablePartition filter(FlowableUnfilteredPartition data, int nowInSec) {
      boolean enforceStrictLiveness = data.metadata().enforceStrictLiveness();
      return FlowablePartition.create(data.header(), filterStaticRow(data.staticRow(), nowInSec, enforceStrictLiveness), filteredContent(data, nowInSec));
   }

   public static Flow<FlowablePartition> filterAndSkipEmpty(FlowableUnfilteredPartition data, int nowInSec) {
      Row staticRow = filterStaticRow(data.staticRow(), nowInSec, data.metadata().enforceStrictLiveness());
      Flow<Row> content = filteredContent(data, nowInSec);
      return !staticRow.isEmpty()?Flow.just(FlowablePartition.create(data.header(), staticRow, content)):content.skipMapEmpty((c) -> {
         return FlowablePartition.create(data.header(), Rows.EMPTY_STATIC_ROW, c);
      });
   }

   public static Flow<FlowablePartition> skipEmpty(FlowablePartition data) {
      return data.staticRow().isEmpty()?data.content().skipMapEmpty((c) -> {
         return FlowablePartition.create(data.header(), Rows.EMPTY_STATIC_ROW, c);
      }):Flow.just(data);
   }

   private static Flow<Row> filteredContent(FlowableUnfilteredPartition data, int nowInSec) {
      return data.content().skippingMap((unfiltered) -> {
         return unfiltered.isRow()?((Row)unfiltered).purge(DeletionPurger.PURGE_ALL, nowInSec, data.metadata().enforceStrictLiveness()):null;
      });
   }

   public static Flow<FlowablePartition> filter(Flow<FlowableUnfilteredPartition> data, int nowInSec) {
      return data.map((p) -> {
         return filter(p, nowInSec);
      });
   }

   public static Flow<FlowablePartition> filterAndSkipEmpty(Flow<FlowableUnfilteredPartition> data, int nowInSec) {
      return data.flatMap((p) -> {
         return filterAndSkipEmpty(p, nowInSec);
      });
   }

   public static Flow<FlowableUnfilteredPartition> skipEmptyUnfilteredPartitions(Flow<FlowableUnfilteredPartition> partitions) {
      return partitions.flatMap((partition) -> {
         return partition.staticRow().isEmpty() && partition.partitionLevelDeletion().isLive()?partition.content().skipMapEmpty((content) -> {
            return FlowableUnfilteredPartition.create(partition.header(), partition.staticRow(), content);
         }):Flow.just(partition);
      });
   }

   public static Flow<FlowablePartition> skipEmptyPartitions(Flow<FlowablePartition> partitions) {
      return partitions.flatMap(FlowablePartitions::skipEmpty);
   }

   public static Flow<FlowablePartition> mergeAndFilter(List<Flow<FlowableUnfilteredPartition>> results, int nowInSec, FlowablePartitions.MergeListener listener) {
      return filterAndSkipEmpty(mergePartitions(results, nowInSec, listener), nowInSec);
   }

   public static Flow<Row> allRows(Flow<FlowablePartition> data) {
      return data.flatMap((partition) -> {
         return partition.content();
      });
   }

   public interface MergeListener {
      FlowablePartitions.MergeListener NONE = new FlowablePartitions.MergeListener() {
         public UnfilteredRowIterators.MergeListener getRowMergeListener(DecoratedKey partitionKey, FlowableUnfilteredPartition[] versions) {
            return null;
         }
      };

      UnfilteredRowIterators.MergeListener getRowMergeListener(DecoratedKey var1, FlowableUnfilteredPartition[] var2);

      default void close() {
      }
   }

   private static class FromRowIterator extends FlowablePartition.FlowSource implements Runnable {
      private final RowIterator iter;
      private final StagedScheduler callOn;

      public FromRowIterator(RowIterator iter, StagedScheduler callOn) {
         super(new PartitionHeader(iter.metadata(), iter.partitionKey(), DeletionTime.LIVE, iter.columns(), iter.isReverseOrder(), EncodingStats.NO_STATS), iter.staticRow());
         this.iter = iter;
         this.callOn = callOn;
      }

      public void requestFirst(FlowSubscriber<Row> subscriber, FlowSubscriptionRecipient subscriptionRecipient) {
         super.subscribe(subscriber, subscriptionRecipient);
         if(this.iter.hasNext()) {
            this.requestNext();
         } else {
            subscriber.onComplete();
         }

      }

      public void requestNext() {
         if(this.callOn == null) {
            this.run();
         } else {
            this.callOn.execute(this, TPCTaskType.READ_FROM_ITERATOR);
         }

      }

      public void run() {
         Row next = (Row)this.iter.next();
         if(this.iter.hasNext()) {
            this.subscriber.onNext(next);
         } else {
            this.subscriber.onFinal(next);
         }

      }

      public void unused() throws Exception {
         this.close();
      }

      public void close() throws Exception {
         this.iter.close();
      }
   }

   private static class FromUnfilteredRowIterator extends FlowableUnfilteredPartition.FlowSource {
      private final UnfilteredRowIterator iter;

      public FromUnfilteredRowIterator(UnfilteredRowIterator iter) {
         super(new PartitionHeader(iter.metadata(), iter.partitionKey(), iter.partitionLevelDeletion(), iter.columns(), iter.isReverseOrder(), iter.stats()), iter.staticRow());
         this.iter = iter;
      }

      public void requestFirst(FlowSubscriber<Unfiltered> subscriber, FlowSubscriptionRecipient subscriptionRecipient) {
         super.subscribe(subscriber, subscriptionRecipient);
         if(this.iter.hasNext()) {
            this.requestNext();
         } else {
            subscriber.onComplete();
         }

      }

      public void requestNext() {
         Unfiltered next = (Unfiltered)this.iter.next();
         if(this.iter.hasNext()) {
            this.subscriber.onNext(next);
         } else {
            this.subscriber.onFinal(next);
         }

      }

      public void unused() throws Exception {
         this.close();
      }

      public void close() throws Exception {
         this.iter.close();
      }
   }

   static class BaseRowIterator<T> implements PartitionTrait {
      final PartitionTrait source;
      final CloseableIterator<T> iter;

      BaseRowIterator(PartitionTrait source, CloseableIterator<T> iter) {
         this.source = source;
         this.iter = iter;
      }

      public TableMetadata metadata() {
         return this.source.metadata();
      }

      public boolean isReverseOrder() {
         return this.source.isReverseOrder();
      }

      public RegularAndStaticColumns columns() {
         return this.source.columns();
      }

      public DecoratedKey partitionKey() {
         return this.source.partitionKey();
      }

      public Row staticRow() {
         return this.source.staticRow();
      }

      public DeletionTime partitionLevelDeletion() {
         return this.source.partitionLevelDeletion();
      }

      public EncodingStats stats() {
         return this.source.stats();
      }

      public void close() {
         this.iter.close();
      }

      public boolean hasNext() {
         return this.iter.hasNext();
      }

      public T next() {
         return this.iter.next();
      }
   }
}
