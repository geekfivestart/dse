package org.apache.cassandra.db.compaction;

import com.google.common.collect.Ordering;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.function.LongPredicate;
import java.util.function.Predicate;
import org.apache.cassandra.db.Clusterable;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.PurgeFunction;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.db.rows.WrappingUnfilteredRowIterator;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.index.transactions.CompactionTransaction;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.CompactionMetrics;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.TableMetadata;

public class CompactionIterator extends CompactionInfo.Holder implements UnfilteredPartitionIterator {
   private static final long UNFILTERED_TO_UPDATE_PROGRESS = 100L;
   private final OperationType type;
   private final CompactionController controller;
   private final List<ISSTableScanner> scanners;
   private final int nowInSec;
   private final UUID compactionId;
   private final long totalBytes;
   private long bytesRead;
   private long totalSourceCQLRows;
   private final long[] mergeCounters;
   private final UnfilteredPartitionIterator compacted;
   private final CompactionMetrics metrics;

   public CompactionIterator(OperationType type, List<ISSTableScanner> scanners, CompactionController controller, int nowInSec, UUID compactionId) {
      this(type, scanners, controller, nowInSec, compactionId, (CompactionMetrics)null);
   }

   public CompactionIterator(OperationType type, List<ISSTableScanner> scanners, CompactionController controller, int nowInSec, UUID compactionId, CompactionMetrics metrics) {
      this.controller = controller;
      this.type = type;
      this.scanners = scanners;
      this.nowInSec = nowInSec;
      this.compactionId = compactionId;
      this.bytesRead = 0L;
      long bytes = 0L;

      ISSTableScanner scanner;
      for(Iterator var9 = scanners.iterator(); var9.hasNext(); bytes += scanner.getLengthInBytes()) {
         scanner = (ISSTableScanner)var9.next();
      }

      this.totalBytes = bytes;
      this.mergeCounters = new long[scanners.size()];
      this.metrics = metrics;
      if(metrics != null) {
         metrics.beginCompaction(this);
      }

      UnfilteredPartitionIterator merged = scanners.isEmpty()?EmptyIterators.unfilteredPartition(controller.cfs.metadata()):UnfilteredPartitionIterators.merge(scanners, nowInSec, this.listener());
      merged = Transformation.apply((UnfilteredPartitionIterator)merged, new CompactionIterator.GarbageSkipper(controller, nowInSec));
      this.compacted = Transformation.apply((UnfilteredPartitionIterator)merged, new CompactionIterator.Purger(controller, nowInSec));
   }

   public TableMetadata metadata() {
      return this.controller.cfs.metadata();
   }

   public CompactionInfo getCompactionInfo() {
      return new CompactionInfo(this.controller.cfs.metadata(), this.type, this.bytesRead, this.totalBytes, this.compactionId);
   }

   private void updateCounterFor(int rows) {
      assert rows > 0 && rows - 1 < this.mergeCounters.length;

      ++this.mergeCounters[rows - 1];
   }

   public long[] getMergedRowCounts() {
      return this.mergeCounters;
   }

   public long getTotalSourceCQLRows() {
      return this.totalSourceCQLRows;
   }

   private UnfilteredPartitionIterators.MergeListener listener() {
      return new UnfilteredPartitionIterators.MergeListener() {
         boolean callOnTrivialMerge;

         {
            this.callOnTrivialMerge = CompactionIterator.this.controller.cfs.indexManager.listIndexes().stream().anyMatch((index) -> {
               return index.getIndexMetadata().isCustom();
            });
         }

         public UnfilteredRowIterators.MergeListener getRowMergeListener(DecoratedKey partitionKey, List<UnfilteredRowIterator> versions) {
            int merged = 0;
            Iterator var4 = versions.iterator();

            while(var4.hasNext()) {
               UnfilteredRowIterator iter = (UnfilteredRowIterator)var4.next();
               if(iter != null) {
                  ++merged;
               }
            }

            assert merged > 0;

            CompactionIterator.this.updateCounterFor(merged);
            if(CompactionIterator.this.type == OperationType.COMPACTION && CompactionIterator.this.controller.cfs.indexManager.hasIndexes()) {
               Columns statics = Columns.NONE;
               Columns regulars = Columns.NONE;
               Iterator var6 = versions.iterator();

               while(var6.hasNext()) {
                  UnfilteredRowIterator iterx = (UnfilteredRowIterator)var6.next();
                  if(iterx != null) {
                     statics = statics.mergeTo(iterx.columns().statics);
                     regulars = regulars.mergeTo(iterx.columns().regulars);
                  }
               }

               RegularAndStaticColumns regularAndStaticColumns = new RegularAndStaticColumns(statics, regulars);
               final CompactionTransaction indexTransaction = CompactionIterator.this.controller.cfs.indexManager.newCompactionTransaction(partitionKey, regularAndStaticColumns, versions.size(), CompactionIterator.this.nowInSec);
               return new UnfilteredRowIterators.MergeListener() {
                  public void onMergedPartitionLevelDeletion(DeletionTime mergedDeletion, DeletionTime[] versions) {
                  }

                  public void onMergedRows(Row merged, Row[] versions) {
                     indexTransaction.start();
                     indexTransaction.onRowMerge(merged, versions);
                     indexTransaction.commit().blockingAwait();
                  }

                  public void onMergedRangeTombstoneMarkers(RangeTombstoneMarker mergedMarker, RangeTombstoneMarker[] versions) {
                  }

                  public void close() {
                  }
               };
            } else {
               return null;
            }
         }

         public void close() {
         }

         public boolean callOnTrivialMerge() {
            return this.callOnTrivialMerge;
         }
      };
   }

   private void updateBytesRead() {
      long n = 0L;

      ISSTableScanner scanner;
      for(Iterator var3 = this.scanners.iterator(); var3.hasNext(); n += scanner.getCurrentPosition()) {
         scanner = (ISSTableScanner)var3.next();
      }

      this.bytesRead = n;
   }

   public boolean hasNext() {
      return this.compacted.hasNext();
   }

   public UnfilteredRowIterator next() {
      return (UnfilteredRowIterator)this.compacted.next();
   }

   public void remove() {
      throw new UnsupportedOperationException();
   }

   public void close() {
      try {
         this.compacted.close();
      } finally {
         if(this.metrics != null) {
            this.metrics.finishCompaction(this);
         }

      }

   }

   public String toString() {
      return this.getCompactionInfo().toString();
   }

   protected boolean maybeStop(com.google.common.base.Predicate<SSTableReader> predicate) {
      return this.controller.getCompacting().stream().anyMatch((s) -> {
         return predicate.apply(s);
      });
   }

   private static class GarbageSkipper extends Transformation<UnfilteredRowIterator> {
      final int nowInSec;
      final CompactionController controller;
      final boolean cellLevelGC;

      private GarbageSkipper(CompactionController controller, int nowInSec) {
         this.controller = controller;
         this.nowInSec = nowInSec;
         this.cellLevelGC = controller.tombstoneOption == CompactionParams.TombstoneOption.CELL;
      }

      protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition) {
         Iterable<UnfilteredRowIterator> sources = this.controller.shadowSources(partition.partitionKey(), !this.cellLevelGC);
         if(sources == null) {
            return partition;
         } else {
            List<UnfilteredRowIterator> iters = new ArrayList();
            Iterator var4 = sources.iterator();

            while(var4.hasNext()) {
               UnfilteredRowIterator iter = (UnfilteredRowIterator)var4.next();
               if(!iter.isEmpty()) {
                  iters.add(iter);
               } else {
                  iter.close();
               }
            }

            if(iters.isEmpty()) {
               return partition;
            } else {
               return new CompactionIterator.GarbageSkippingUnfilteredRowIterator(partition, UnfilteredRowIterators.merge(iters, this.nowInSec), this.nowInSec, this.cellLevelGC);
            }
         }
      }
   }

   private static class GarbageSkippingUnfilteredRowIterator extends WrappingUnfilteredRowIterator {
      final UnfilteredRowIterator tombSource;
      final DeletionTime partitionLevelDeletion;
      final Row staticRow;
      final ColumnFilter cf;
      final int nowInSec;
      final TableMetadata metadata;
      final boolean cellLevelGC;
      DeletionTime tombOpenDeletionTime;
      DeletionTime dataOpenDeletionTime;
      DeletionTime openDeletionTime;
      DeletionTime partitionDeletionTime;
      DeletionTime activeDeletionTime;
      Unfiltered tombNext;
      Unfiltered dataNext;
      Unfiltered next;

      protected GarbageSkippingUnfilteredRowIterator(UnfilteredRowIterator dataSource, UnfilteredRowIterator tombSource, int nowInSec, boolean cellLevelGC) {
         super(dataSource);
         this.tombOpenDeletionTime = DeletionTime.LIVE;
         this.dataOpenDeletionTime = DeletionTime.LIVE;
         this.openDeletionTime = DeletionTime.LIVE;
         this.tombNext = null;
         this.dataNext = null;
         this.next = null;
         this.tombSource = tombSource;
         this.nowInSec = nowInSec;
         this.cellLevelGC = cellLevelGC;
         this.metadata = dataSource.metadata();
         this.cf = ColumnFilter.all(this.metadata);
         this.activeDeletionTime = this.partitionDeletionTime = tombSource.partitionLevelDeletion();
         this.partitionLevelDeletion = dataSource.partitionLevelDeletion().supersedes(tombSource.partitionLevelDeletion())?dataSource.partitionLevelDeletion():DeletionTime.LIVE;
         Row dataStaticRow = this.garbageFilterRow(dataSource.staticRow(), tombSource.staticRow());
         this.staticRow = dataStaticRow != null?dataStaticRow:Rows.EMPTY_STATIC_ROW;
         this.tombNext = advance(tombSource);
         this.dataNext = advance(dataSource);
      }

      private static Unfiltered advance(UnfilteredRowIterator source) {
         return source.hasNext()?(Unfiltered)source.next():null;
      }

      public DeletionTime partitionLevelDeletion() {
         return this.partitionLevelDeletion;
      }

      public void close() {
         super.close();
         this.tombSource.close();
      }

      public Row staticRow() {
         return this.staticRow;
      }

      public boolean hasNext() {
         while(this.next == null && this.dataNext != null) {
            int cmp = this.tombNext == null?-1:this.metadata.comparator.compare((Clusterable)this.dataNext, (Clusterable)this.tombNext);
            if(cmp < 0) {
               if(this.dataNext.isRow()) {
                  this.next = ((Row)this.dataNext).filter(this.cf, this.activeDeletionTime, false, this.metadata);
               } else {
                  this.next = this.processDataMarker();
               }
            } else if(cmp == 0) {
               if(this.dataNext.isRow()) {
                  this.next = this.garbageFilterRow((Row)this.dataNext, (Row)this.tombNext);
               } else {
                  this.tombOpenDeletionTime = this.updateOpenDeletionTime(this.tombOpenDeletionTime, this.tombNext);
                  this.activeDeletionTime = (DeletionTime)Ordering.natural().max(this.partitionDeletionTime, this.tombOpenDeletionTime);
                  this.next = this.processDataMarker();
               }
            } else if(this.tombNext.isRangeTombstoneMarker()) {
               this.tombOpenDeletionTime = this.updateOpenDeletionTime(this.tombOpenDeletionTime, this.tombNext);
               this.activeDeletionTime = (DeletionTime)Ordering.natural().max(this.partitionDeletionTime, this.tombOpenDeletionTime);
               boolean supersededBefore = this.openDeletionTime.isLive();
               boolean supersededAfter = !this.dataOpenDeletionTime.supersedes(this.activeDeletionTime);
               if(supersededBefore && !supersededAfter) {
                  this.next = new RangeTombstoneBoundMarker(((RangeTombstoneMarker)this.tombNext).closeBound(false).invert(), this.dataOpenDeletionTime);
               }
            }

            if(this.next instanceof RangeTombstoneMarker) {
               this.openDeletionTime = this.updateOpenDeletionTime(this.openDeletionTime, this.next);
            }

            if(cmp <= 0) {
               this.dataNext = advance(this.wrapped);
            }

            if(cmp >= 0) {
               this.tombNext = advance(this.tombSource);
            }
         }

         return this.next != null;
      }

      protected Row garbageFilterRow(Row dataRow, Row tombRow) {
         if(this.cellLevelGC) {
            return Rows.removeShadowedCells(dataRow, tombRow, this.activeDeletionTime, this.nowInSec);
         } else {
            DeletionTime deletion = (DeletionTime)Ordering.natural().max(tombRow.deletion().time(), this.activeDeletionTime);
            return dataRow.filter(this.cf, deletion, false, this.metadata);
         }
      }

      private RangeTombstoneMarker processDataMarker() {
         this.dataOpenDeletionTime = this.updateOpenDeletionTime(this.dataOpenDeletionTime, this.dataNext);
         boolean supersededBefore = this.openDeletionTime.isLive();
         boolean supersededAfter = !this.dataOpenDeletionTime.supersedes(this.activeDeletionTime);
         RangeTombstoneMarker marker = (RangeTombstoneMarker)this.dataNext;
         return (RangeTombstoneMarker)(!supersededBefore?(!supersededAfter?marker:new RangeTombstoneBoundMarker(marker.closeBound(false), marker.closeDeletionTime(false))):(!supersededAfter?new RangeTombstoneBoundMarker(marker.openBound(false), marker.openDeletionTime(false)):null));
      }

      public Unfiltered next() {
         if(!this.hasNext()) {
            throw new IllegalStateException();
         } else {
            Unfiltered v = this.next;
            this.next = null;
            return v;
         }
      }

      private DeletionTime updateOpenDeletionTime(DeletionTime openDeletionTime, Unfiltered next) {
         RangeTombstoneMarker marker = (RangeTombstoneMarker)next;

         assert openDeletionTime.isLive() == !marker.isClose(false);

         assert openDeletionTime.isLive() || openDeletionTime.equals(marker.closeDeletionTime(false));

         return marker.isOpen(false)?marker.openDeletionTime(false):DeletionTime.LIVE;
      }
   }

   private class Purger extends PurgeFunction {
      private final CompactionController controller;
      private DecoratedKey currentKey;
      private LongPredicate purgeEvaluator;
      private long compactedUnfiltered;

      private Purger(CompactionController controller, int nowInSec) {
         super(nowInSec, controller.gcBefore, controller.compactingRepaired()?2147483647:-2147483648, controller.cfs.getCompactionStrategyManager().onlyPurgeRepairedTombstones(), controller.cfs.metadata.get().enforceStrictLiveness());
         this.controller = controller;
      }

      protected void onEmptyPartitionPostPurge(DecoratedKey key) {
         if(CompactionIterator.this.type == OperationType.COMPACTION) {
            this.controller.cfs.invalidateCachedPartition(key);
         }

      }

      protected void onNewPartition(DecoratedKey key) {
         this.currentKey = key;
         this.purgeEvaluator = null;
      }

      protected void updateProgress() {
         CompactionIterator.this.totalSourceCQLRows++;
         if(++this.compactedUnfiltered % 100L == 0L) {
            CompactionIterator.this.updateBytesRead();
         }

      }

      protected LongPredicate getPurgeEvaluator() {
         if(this.purgeEvaluator == null) {
            this.purgeEvaluator = this.controller.getPurgeEvaluator(this.currentKey);
         }

         return this.purgeEvaluator;
      }
   }
}
