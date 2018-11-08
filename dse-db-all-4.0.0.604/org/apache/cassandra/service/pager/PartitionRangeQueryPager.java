package org.apache.cassandra.service.pager;

import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.ExcludingBounds;
import org.apache.cassandra.dht.IncludingExcludingBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;

public class PartitionRangeQueryPager extends AbstractQueryPager<PartitionRangeReadCommand> {
   private volatile DecoratedKey lastReturnedKey;
   private volatile PagingState.RowMark lastReturnedRow;

   public PartitionRangeQueryPager(PartitionRangeReadCommand command, PagingState state, ProtocolVersion protocolVersion) {
      super(command, protocolVersion);
      if(state != null) {
         this.lastReturnedKey = command.metadata().partitioner.decorateKey(state.partitionKey);
         this.lastReturnedRow = state.rowMark;
         this.restoreState(this.lastReturnedKey, state.remaining, state.remainingInPartition, state.inclusive);
      }

   }

   private PartitionRangeQueryPager(PartitionRangeReadCommand command, ProtocolVersion protocolVersion, DecoratedKey lastReturnedKey, PagingState.RowMark lastReturnedRow, int remaining, int remainingInPartition) {
      super(command, protocolVersion);
      this.lastReturnedKey = lastReturnedKey;
      this.lastReturnedRow = lastReturnedRow;
      this.restoreState(lastReturnedKey, remaining, remainingInPartition, false);
   }

   public PartitionRangeQueryPager withUpdatedLimit(DataLimits newLimits) {
      return new PartitionRangeQueryPager(((PartitionRangeReadCommand)this.command).withUpdatedLimit(newLimits), this.protocolVersion, this.lastReturnedKey, this.lastReturnedRow, this.maxRemaining(), this.remainingInPartition());
   }

   protected PagingState makePagingState(DecoratedKey lastKey, Row lastRow, boolean inclusive) {
      return this.makePagingState(this.getLastReturnedKey(lastKey, lastRow), this.getLastReturnedRow(lastRow), inclusive);
   }

   protected PagingState makePagingState(boolean inclusive) {
      return this.makePagingState(this.lastReturnedKey, this.lastReturnedRow, inclusive);
   }

   private PagingState makePagingState(DecoratedKey lastKey, PagingState.RowMark lastRow, boolean inclusive) {
      int maxRemaining = inclusive?FBUtilities.add(this.maxRemaining(), 1):this.maxRemaining();
      int remainingInPartition = inclusive?FBUtilities.add(this.remainingInPartition(), 1):this.remainingInPartition();
      return lastKey == null?null:new PagingState(lastKey.getKey(), lastRow, maxRemaining, remainingInPartition, inclusive);
   }

   protected ReadCommand nextPageReadCommand(DataLimits limits, PageSize pageSize) throws RequestExecutionException {
      DataRange fullRange = ((PartitionRangeReadCommand)this.command).dataRange();
      DataRange pageRange;
      if(this.lastReturnedKey == null) {
         pageRange = fullRange;
         limits = limits.forPaging(pageSize);
      } else {
         if(this.lastReturnedKey.equals(fullRange.keyRange().right) && this.remainingInPartition() == 0 && this.lastReturnedRow == null) {
            return null;
         }

         boolean includeLastKey = this.remainingInPartition() > 0 && this.lastReturnedRow != null;
         AbstractBounds<PartitionPosition> bounds = this.makeKeyBounds(this.lastReturnedKey, includeLastKey || this.inclusive);
         if(includeLastKey) {
            pageRange = fullRange.forPaging(bounds, ((PartitionRangeReadCommand)this.command).metadata().comparator, this.lastReturnedRow.clustering(((PartitionRangeReadCommand)this.command).metadata()), this.inclusive);
            limits = limits.forPaging(pageSize, this.lastReturnedKey.getKey(), this.remainingInPartition());
         } else {
            pageRange = fullRange.forSubRange(bounds);
            limits = limits.forPaging(pageSize);
         }
      }

      return ((PartitionRangeReadCommand)this.command).withUpdatedLimitsAndDataRange(limits, pageRange);
   }

   protected void recordLast(DecoratedKey key, Row last) {
      this.lastReturnedKey = this.getLastReturnedKey(key, last);
      this.lastReturnedRow = this.getLastReturnedRow(last);
   }

   private DecoratedKey getLastReturnedKey(DecoratedKey key, Row last) {
      return last != null?key:this.lastReturnedKey;
   }

   private PagingState.RowMark getLastReturnedRow(Row last) {
      return last != null && last.clustering() != Clustering.STATIC_CLUSTERING?PagingState.RowMark.create(((PartitionRangeReadCommand)this.command).metadata(), last, this.protocolVersion):this.lastReturnedRow;
   }

   protected boolean isPreviouslyReturnedPartition(DecoratedKey key) {
      return key.equals(this.lastReturnedKey);
   }

   private AbstractBounds<PartitionPosition> makeKeyBounds(PartitionPosition lastReturnedKey, boolean includeLastKey) {
      AbstractBounds<PartitionPosition> bounds = ((PartitionRangeReadCommand)this.command).dataRange().keyRange();
      return (AbstractBounds)(!(bounds instanceof Range) && !(bounds instanceof Bounds)?(includeLastKey?new IncludingExcludingBounds(lastReturnedKey, bounds.right):new ExcludingBounds(lastReturnedKey, bounds.right)):(includeLastKey?new Bounds(lastReturnedKey, bounds.right):new Range(lastReturnedKey, bounds.right)));
   }
}
