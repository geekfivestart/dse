package org.apache.cassandra.db.rows;

import java.util.Comparator;
import java.util.Iterator;
import org.apache.cassandra.db.Clusterable;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.schema.TableMetadata;

public class RowAndDeletionMergeIterator extends AbstractUnfilteredRowIterator {
   private final boolean removeShadowedData;
   private final Comparator<Clusterable> comparator;
   private final ColumnFilter selection;
   private final Iterator<Row> rows;
   private Row nextRow;
   private final Iterator<RangeTombstone> ranges;
   private RangeTombstone nextRange;
   private RangeTombstone openRange;

   public RowAndDeletionMergeIterator(TableMetadata metadata, DecoratedKey partitionKey, DeletionTime partitionLevelDeletion, ColumnFilter selection, Row staticRow, boolean isReversed, EncodingStats stats, Iterator<Row> rows, Iterator<RangeTombstone> ranges, boolean removeShadowedData) {
      super(metadata, partitionKey, partitionLevelDeletion, selection.fetchedColumns(), staticRow, isReversed, stats);
      this.comparator = (Comparator)(isReversed?metadata.comparator.reversed():metadata.comparator);
      this.selection = selection;
      this.removeShadowedData = removeShadowedData;
      this.rows = rows;
      this.ranges = ranges;
   }

   protected Unfiltered computeNext() {
      while(true) {
         this.updateNextRow();
         if(this.nextRow == null) {
            if(this.openRange != null) {
               return this.closeOpenedRange();
            }

            this.updateNextRange();
            return (Unfiltered)(this.nextRange == null?(Unfiltered)this.endOfData():this.openRange());
         }

         Row row;
         if(this.openRange == null) {
            this.updateNextRange();
            if(this.nextRange != null && this.comparator.compare(this.openBound(this.nextRange), this.nextRow.clustering()) < 0) {
               return this.openRange();
            }

            row = this.consumeNextRow();
            if(row != null) {
               return row;
            }
         } else {
            if(this.comparator.compare(this.closeBound(this.openRange), this.nextRow.clustering()) < 0) {
               return this.closeOpenedRange();
            }

            row = this.consumeNextRow();
            if(row != null) {
               return row;
            }
         }
      }
   }

   private void updateNextRow() {
      if(this.nextRow == null && this.rows.hasNext()) {
         this.nextRow = (Row)this.rows.next();
      }

   }

   private void updateNextRange() {
      while(this.nextRange == null && this.ranges.hasNext()) {
         this.nextRange = (RangeTombstone)this.ranges.next();
         if(this.removeShadowedData && this.partitionLevelDeletion().supersedes(this.nextRange.deletionTime())) {
            this.nextRange = null;
         }
      }

   }

   private Row consumeNextRow() {
      Row row = this.nextRow;
      this.nextRow = null;
      if(!this.removeShadowedData) {
         return row.filter(this.selection, this.metadata());
      } else {
         DeletionTime activeDeletion = this.openRange == null?this.partitionLevelDeletion():this.openRange.deletionTime();
         return row.filter(this.selection, activeDeletion, false, this.metadata());
      }
   }

   private RangeTombstone consumeNextRange() {
      RangeTombstone range = this.nextRange;
      this.nextRange = null;
      return range;
   }

   private RangeTombstone consumeOpenRange() {
      RangeTombstone range = this.openRange;
      this.openRange = null;
      return range;
   }

   private ClusteringBound openBound(RangeTombstone range) {
      return range.deletedSlice().open(this.isReverseOrder());
   }

   private ClusteringBound closeBound(RangeTombstone range) {
      return range.deletedSlice().close(this.isReverseOrder());
   }

   private RangeTombstoneMarker closeOpenedRange() {
      this.updateNextRange();
      Object marker;
      if(this.nextRange != null && this.comparator.compare(this.closeBound(this.openRange), this.openBound(this.nextRange)) == 0) {
         marker = RangeTombstoneBoundaryMarker.makeBoundary(this.isReverseOrder(), this.closeBound(this.openRange), this.openBound(this.nextRange), this.openRange.deletionTime(), this.nextRange.deletionTime());
         this.openRange = this.consumeNextRange();
      } else {
         RangeTombstone toClose = this.consumeOpenRange();
         marker = new RangeTombstoneBoundMarker(this.closeBound(toClose), toClose.deletionTime());
      }

      return (RangeTombstoneMarker)marker;
   }

   private RangeTombstoneMarker openRange() {
      assert this.openRange == null && this.nextRange != null;

      this.openRange = this.consumeNextRange();
      return new RangeTombstoneBoundMarker(this.openBound(this.openRange), this.openRange.deletionTime());
   }
}
