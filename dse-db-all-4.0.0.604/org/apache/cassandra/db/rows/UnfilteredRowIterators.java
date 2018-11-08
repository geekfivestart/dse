package org.apache.cassandra.db.rows;

import com.google.common.hash.Hasher;
import io.reactivex.functions.Function;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import org.apache.cassandra.db.Clusterable;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.DigestVersion;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.transform.FilteredRows;
import org.apache.cassandra.db.transform.MoreRows;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.HashingUtils;
import org.apache.cassandra.utils.IMergeIterator;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.Reducer;
import org.apache.cassandra.utils.flow.Flow;
import org.slf4j.LoggerFactory;

public abstract class UnfilteredRowIterators {
   private static final org.slf4j.Logger logger = LoggerFactory.getLogger(UnfilteredRowIterators.class);

   private UnfilteredRowIterators() {
   }

   public static RowIterator filter(UnfilteredRowIterator iter, int nowInSec) {
      return FilteredRows.filter(iter, nowInSec);
   }

   public static UnfilteredRowIterator merge(List<UnfilteredRowIterator> iterators, int nowInSec) {
      assert !iterators.isEmpty();

      return (UnfilteredRowIterator)(iterators.size() == 1?(UnfilteredRowIterator)iterators.get(0):UnfilteredRowIterators.UnfilteredRowMergeIterator.create(iterators, nowInSec, (UnfilteredRowIterators.MergeListener)null));
   }

   public static UnfilteredRowIterator merge(List<UnfilteredRowIterator> iterators, int nowInSec, UnfilteredRowIterators.MergeListener mergeListener) {
      return UnfilteredRowIterators.UnfilteredRowMergeIterator.create(iterators, nowInSec, mergeListener);
   }

   public static UnfilteredRowIterator noRowsIterator(TableMetadata metadata, DecoratedKey partitionKey, Row staticRow, DeletionTime partitionDeletion, boolean isReverseOrder) {
      return EmptyIterators.unfilteredRow(metadata, partitionKey, isReverseOrder, staticRow, partitionDeletion);
   }

   public static void digest(UnfilteredRowIterator iterator, Hasher hasher, DigestVersion version) {
      digestPartition(iterator, hasher, version);

      while(iterator.hasNext()) {
         digestUnfiltered((Unfiltered)iterator.next(), hasher);
      }

   }

   public static Flow<Void> digest(FlowableUnfilteredPartition partition, Hasher hasher, DigestVersion version) {
      if(partition.header().isEmpty() && partition.staticRow().isEmpty()) {
         return partition.content().reduce(Boolean.valueOf(true), (first, unfiltered) -> {
            if(first.booleanValue()) {
               digestPartition(partition, hasher, version);
            }

            digestUnfiltered(unfiltered, hasher);
            return Boolean.valueOf(false);
         }).map((b) -> {
            return null;
         });
      } else {
         digestPartition(partition, hasher, version);
         return partition.content().process((unfiltered) -> {
            digestUnfiltered(unfiltered, hasher);
         });
      }
   }

   public static Hasher digestPartition(PartitionTrait partition, Hasher hasher, DigestVersion version) {
      HashingUtils.updateBytes(hasher, partition.partitionKey().getKey().duplicate());
      partition.partitionLevelDeletion().digest(hasher);
      partition.columns().regulars.digest(hasher);
      if(partition.staticRow() != Rows.EMPTY_STATIC_ROW) {
         partition.columns().statics.digest(hasher);
      }

      HashingUtils.updateWithBoolean(hasher, partition.isReverseOrder());
      partition.staticRow().digest(hasher);
      return hasher;
   }

   public static Hasher digestUnfiltered(Unfiltered unfiltered, Hasher hasher) {
      unfiltered.digest(hasher);
      return hasher;
   }

   public static UnfilteredRowIterator withOnlyQueriedData(UnfilteredRowIterator iterator, ColumnFilter filter) {
      return filter.allFetchedColumnsAreQueried()?iterator:Transformation.apply((UnfilteredRowIterator)iterator, new WithOnlyQueriedData(filter));
   }

   public static UnfilteredRowIterator concat(UnfilteredRowIterator iter1, final UnfilteredRowIterator iter2) {
      assert iter1.metadata().id.equals(iter2.metadata().id) && iter1.partitionKey().equals(iter2.partitionKey()) && iter1.partitionLevelDeletion().equals(iter2.partitionLevelDeletion()) && iter1.isReverseOrder() == iter2.isReverseOrder() && iter1.staticRow().equals(iter2.staticRow());

      class Extend implements MoreRows<UnfilteredRowIterator> {
         boolean returned = false;

         Extend() {
         }

         public UnfilteredRowIterator moreContents() {
            if(this.returned) {
               return null;
            } else {
               this.returned = true;
               return iter2;
            }
         }
      }

      return MoreRows.extend(iter1, new Extend(), iter1.columns().mergeTo(iter2.columns()));
   }

   public static UnfilteredRowIterator concat(final Unfiltered first, UnfilteredRowIterator rest) {
      return new WrappingUnfilteredRowIterator(rest) {
         private boolean hasReturnedFirst;

         public boolean hasNext() {
            return this.hasReturnedFirst?super.hasNext():true;
         }

         public Unfiltered next() {
            if(!this.hasReturnedFirst) {
               this.hasReturnedFirst = true;
               return first;
            } else {
               return super.next();
            }
         }
      };
   }

   public static UnfilteredRowIterator withValidation(final UnfilteredRowIterator iterator, final String filename) {
      return Transformation.apply((UnfilteredRowIterator)iterator, new UnfilteredRowIterators.ValidatorTransformation() {
         protected void validate(Unfiltered unfiltered) {
            try {
               unfiltered.validateData(iterator.metadata());
            } catch (MarshalException var3) {
               throw new CorruptSSTableException(var3, filename);
            }
         }
      });
   }

   public static UnfilteredRowIterator withValidation(final UnfilteredRowIterator iterator) {
      return Transformation.apply((UnfilteredRowIterator)iterator, new UnfilteredRowIterators.ValidatorTransformation() {
         protected void validate(Unfiltered unfiltered) {
            unfiltered.validateData(iterator.metadata());
         }
      });
   }

   public static UnfilteredRowIterator loggingIterator(UnfilteredRowIterator iterator, final String id, final boolean fullDetails) {
      final TableMetadata metadata = iterator.metadata();
      logger.info("[{}] Logging iterator on {}.{}, partition key={}, reversed={}, deletion={}", new Object[]{id, metadata.keyspace, metadata.name, metadata.partitionKeyType.getString(iterator.partitionKey().getKey()), Boolean.valueOf(iterator.isReverseOrder()), Long.valueOf(iterator.partitionLevelDeletion().markedForDeleteAt())});
      class Logger extends Transformation {
         Logger() {
         }

         public Row applyToStatic(Row row) {
            if(!row.isEmpty()) {
               UnfilteredRowIterators.logger.info("[{}] {}", id, row.toString(metadata, fullDetails));
            }

            return row;
         }

         public Row applyToRow(Row row) {
            UnfilteredRowIterators.logger.info("[{}] {}", id, row.toString(metadata, fullDetails));
            return row;
         }

         public RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker) {
            UnfilteredRowIterators.logger.info("[{}] {}", id, marker.toString(metadata));
            return marker;
         }
      }

      return Transformation.apply((UnfilteredRowIterator)iterator, new Logger());
   }

   public static class MergeReducer extends Reducer<Unfiltered, Unfiltered> {
      private final UnfilteredRowIterators.MergeListener listener;
      private Unfiltered.Kind nextKind;
      private final boolean reversed;
      private final int size;
      private final Row.Merger rowMerger;
      private RangeTombstoneMarker.Merger markerMerger;
      private final DeletionTime partitionLevelDeletion;

      public MergeReducer(int size, boolean reversed, DeletionTime partitionLevelDeletion, Columns columns, int nowInSec, UnfilteredRowIterators.MergeListener listener) {
         this.reversed = reversed;
         this.size = size;
         this.rowMerger = new Row.Merger(size, nowInSec, columns.size(), columns.hasComplex());
         this.markerMerger = null;
         this.listener = listener;
         this.partitionLevelDeletion = partitionLevelDeletion;
      }

      private void maybeInitMarkerMerger() {
         if(this.markerMerger == null) {
            this.markerMerger = new RangeTombstoneMarker.Merger(this.size, this.partitionLevelDeletion, this.reversed);
         }

      }

      public boolean trivialReduceIsTrivial() {
         return this.listener == null;
      }

      public void reduce(int idx, Unfiltered current) {
         this.nextKind = current.kind();
         if(this.nextKind == Unfiltered.Kind.ROW) {
            this.rowMerger.add(idx, (Row)current);
         } else {
            this.maybeInitMarkerMerger();
            this.markerMerger.add(idx, (RangeTombstoneMarker)current);
         }

      }

      public Unfiltered getReduced() {
         if(this.nextKind == Unfiltered.Kind.ROW) {
            Row merged = this.rowMerger.merge(this.markerMerger == null?this.partitionLevelDeletion:this.markerMerger.activeDeletion());
            if(this.listener != null) {
               this.listener.onMergedRows((Row)(merged == null?ArrayBackedRow.emptyRow(this.rowMerger.mergedClustering()):merged), this.rowMerger.mergedRows());
            }

            return merged;
         } else {
            this.maybeInitMarkerMerger();
            RangeTombstoneMarker merged = this.markerMerger.merge();
            if(this.listener != null) {
               this.listener.onMergedRangeTombstoneMarkers(merged, this.markerMerger.mergedMarkers());
            }

            return merged;
         }
      }

      public void onKeyChange() {
         if(this.nextKind == Unfiltered.Kind.ROW) {
            this.rowMerger.clear();
         } else if(this.markerMerger != null) {
            this.markerMerger.clear();
         }

      }
   }

   private static class UnfilteredRowMergeIterator extends AbstractUnfilteredRowIterator {
      private final IMergeIterator<Unfiltered, Unfiltered> mergeIterator;
      private final UnfilteredRowIterators.MergeListener listener;

      private UnfilteredRowMergeIterator(TableMetadata metadata, List<UnfilteredRowIterator> iterators, RegularAndStaticColumns columns, DeletionTime partitionDeletion, int nowInSec, boolean reversed, UnfilteredRowIterators.MergeListener listener) {
         super(metadata, ((UnfilteredRowIterator)iterators.get(0)).partitionKey(), partitionDeletion, columns, mergeStaticRows(iterators, columns.statics, nowInSec, listener, partitionDeletion), reversed, mergeStats(iterators));
         Comparator<Clusterable> comparator = reversed?metadata.comparator.reversed():metadata.comparator;
         UnfilteredRowIterators.MergeReducer reducer = new UnfilteredRowIterators.MergeReducer(iterators.size(), reversed, partitionDeletion, columns.regulars, nowInSec, listener);
         this.mergeIterator = MergeIterator.get(iterators, (Comparator)comparator, reducer);
         this.listener = listener;
      }

      private static UnfilteredRowIterators.UnfilteredRowMergeIterator create(List<UnfilteredRowIterator> iterators, int nowInSec, UnfilteredRowIterators.MergeListener listener) {
         try {
            checkForInvalidInput(iterators);
            return new UnfilteredRowIterators.UnfilteredRowMergeIterator(((UnfilteredRowIterator)iterators.get(0)).metadata(), iterators, collectColumns(iterators), collectPartitionLevelDeletion(iterators, listener), nowInSec, ((UnfilteredRowIterator)iterators.get(0)).isReverseOrder(), listener);
         } catch (Error | RuntimeException var6) {
            try {
               FBUtilities.closeAll(iterators);
            } catch (Exception var5) {
               var6.addSuppressed(var5);
            }

            throw var6;
         }
      }

      private static void checkForInvalidInput(List<UnfilteredRowIterator> iterators) {
         if(!iterators.isEmpty()) {
            UnfilteredRowIterator first = (UnfilteredRowIterator)iterators.get(0);

            for(int i = 1; i < iterators.size(); ++i) {
               UnfilteredRowIterator iter = (UnfilteredRowIterator)iterators.get(i);

               assert first.metadata().id.equals(iter.metadata().id);

               assert first.partitionKey().equals(iter.partitionKey());

               assert first.isReverseOrder() == iter.isReverseOrder();
            }

         }
      }

      private static DeletionTime collectPartitionLevelDeletion(List<UnfilteredRowIterator> iterators, UnfilteredRowIterators.MergeListener listener) {
         DeletionTime[] versions = listener == null?null:new DeletionTime[iterators.size()];
         DeletionTime delTime = DeletionTime.LIVE;

         for(int i = 0; i < iterators.size(); ++i) {
            UnfilteredRowIterator iter = (UnfilteredRowIterator)iterators.get(i);
            DeletionTime iterDeletion = iter.partitionLevelDeletion();
            if(listener != null) {
               versions[i] = iterDeletion;
            }

            if(!delTime.supersedes(iterDeletion)) {
               delTime = iterDeletion;
            }
         }

         if(listener != null) {
            listener.onMergedPartitionLevelDeletion(delTime, versions);
         }

         return delTime;
      }

      private static Row mergeStaticRows(List<UnfilteredRowIterator> iterators, Columns columns, int nowInSec, UnfilteredRowIterators.MergeListener listener, DeletionTime partitionDeletion) {
         if(columns.isEmpty()) {
            return Rows.EMPTY_STATIC_ROW;
         } else if(iterators.stream().allMatch((iter) -> {
            return iter.staticRow().isEmpty();
         })) {
            return Rows.EMPTY_STATIC_ROW;
         } else {
            Row.Merger merger = new Row.Merger(iterators.size(), nowInSec, columns.size(), columns.hasComplex());

            for(int i = 0; i < iterators.size(); ++i) {
               merger.add(i, ((UnfilteredRowIterator)iterators.get(i)).staticRow());
            }

            Row merged = merger.merge(partitionDeletion);
            if(merged == null) {
               merged = Rows.EMPTY_STATIC_ROW;
            }

            if(listener != null) {
               listener.onMergedRows(merged, merger.mergedRows());
            }

            return merged;
         }
      }

      private static RegularAndStaticColumns collectColumns(List<UnfilteredRowIterator> iterators) {
         RegularAndStaticColumns first = ((UnfilteredRowIterator)iterators.get(0)).columns();
         Columns statics = first.statics;
         Columns regulars = first.regulars;

         for(int i = 1; i < iterators.size(); ++i) {
            RegularAndStaticColumns cols = ((UnfilteredRowIterator)iterators.get(i)).columns();
            statics = statics.mergeTo(cols.statics);
            regulars = regulars.mergeTo(cols.regulars);
         }

         return statics == first.statics && regulars == first.regulars?first:new RegularAndStaticColumns(statics, regulars);
      }

      private static EncodingStats mergeStats(List<UnfilteredRowIterator> iterators) {
         EncodingStats.Merger merger = null;
         Iterator var2 = iterators.iterator();

         while(var2.hasNext()) {
            UnfilteredRowIterator iter = (UnfilteredRowIterator)var2.next();
            EncodingStats stats = iter.stats();
            if(!stats.equals(EncodingStats.NO_STATS)) {
               if(merger == null) {
                  merger = new EncodingStats.Merger(EncodingStats.NO_STATS);
               }

               merger.mergeWith(iter.stats());
            }
         }

         return merger == null?EncodingStats.NO_STATS:merger.get();
      }

      protected Unfiltered computeNext() {
         while(true) {
            if(this.mergeIterator.hasNext()) {
               Unfiltered merged = (Unfiltered)this.mergeIterator.next();
               if(merged == null) {
                  continue;
               }

               return merged;
            }

            return (Unfiltered)this.endOfData();
         }
      }

      public void close() {
         FileUtils.closeQuietly((AutoCloseable)this.mergeIterator);
         if(this.listener != null) {
            this.listener.close();
         }

      }
   }

   private abstract static class ValidatorTransformation extends Transformation {
      private ValidatorTransformation() {
      }

      public Row applyToStatic(Row row) {
         this.validate(row);
         return row;
      }

      public Row applyToRow(Row row) {
         this.validate(row);
         return row;
      }

      public RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker) {
         this.validate(marker);
         return marker;
      }

      protected abstract void validate(Unfiltered var1);
   }

   public interface MergeListener {
      void onMergedPartitionLevelDeletion(DeletionTime var1, DeletionTime[] var2);

      void onMergedRows(Row var1, Row[] var2);

      void onMergedRangeTombstoneMarkers(RangeTombstoneMarker var1, RangeTombstoneMarker[] var2);

      void close();
   }
}
