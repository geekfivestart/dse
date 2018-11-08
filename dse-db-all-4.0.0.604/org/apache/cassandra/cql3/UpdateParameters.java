package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

public class UpdateParameters {
   public final TableMetadata metadata;
   public final RegularAndStaticColumns updatedColumns;
   public final QueryOptions options;
   private final int nowInSec;
   private final long timestamp;
   private final int ttl;
   private final DeletionTime deletionTime;
   private final Map<DecoratedKey, Partition> prefetchedRows;
   private Row.Builder staticBuilder;
   private Row.Builder regularBuilder;
   private Row.Builder builder;

   public UpdateParameters(TableMetadata metadata, RegularAndStaticColumns updatedColumns, QueryOptions options, long timestamp, int ttl, Map<DecoratedKey, Partition> prefetchedRows) throws InvalidRequestException {
      this.metadata = metadata;
      this.updatedColumns = updatedColumns;
      this.options = options;
      this.nowInSec = FBUtilities.nowInSeconds();
      this.timestamp = timestamp;
      this.ttl = ttl;
      this.deletionTime = new DeletionTime(timestamp, this.nowInSec);
      this.prefetchedRows = prefetchedRows;
      if(timestamp == -9223372036854775808L) {
         throw new InvalidRequestException(String.format("Out of bound timestamp, must be in [%d, %d]", new Object[]{Long.valueOf(-9223372036854775807L), Long.valueOf(9223372036854775807L)}));
      }
   }

   public void newRow(Clustering clustering) throws InvalidRequestException {
      if(this.metadata.isDense() && !this.metadata.isCompound()) {
         assert clustering.size() == 1;

         ByteBuffer value = clustering.get(0);
         if(value == null || !value.hasRemaining()) {
            throw new InvalidRequestException("Invalid empty or null value for column " + ((ColumnMetadata)this.metadata.clusteringColumns().get(0)).name);
         }
      }

      if(clustering == Clustering.STATIC_CLUSTERING) {
         if(this.staticBuilder == null) {
            this.staticBuilder = Row.Builder.unsorted(this.nowInSec);
         }

         this.builder = this.staticBuilder;
      } else {
         if(this.regularBuilder == null) {
            this.regularBuilder = Row.Builder.unsorted(this.nowInSec);
         }

         this.builder = this.regularBuilder;
      }

      this.builder.newRow(clustering);
   }

   public Clustering currentClustering() {
      return this.builder.clustering();
   }

   public void addPrimaryKeyLivenessInfo() {
      this.builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(this.timestamp, this.ttl, this.nowInSec));
   }

   public void addRowDeletion() {
      if(this.metadata.isCompactTable() && this.builder.clustering() != Clustering.STATIC_CLUSTERING) {
         this.addTombstone(this.metadata.compactValueColumn);
      } else {
         this.builder.addRowDeletion(Row.Deletion.regular(this.deletionTime));
      }

   }

   public void addTombstone(ColumnMetadata column) throws InvalidRequestException {
      this.addTombstone(column, (CellPath)null);
   }

   public void addTombstone(ColumnMetadata column, CellPath path) throws InvalidRequestException {
      this.builder.addCell(BufferCell.tombstone(column, this.timestamp, this.nowInSec, path));
   }

   public void addCell(ColumnMetadata column, ByteBuffer value) throws InvalidRequestException {
      this.addCell(column, (CellPath)null, value);
   }

   public void addCell(ColumnMetadata column, CellPath path, ByteBuffer value) throws InvalidRequestException {
      Cell cell = this.ttl == 0?BufferCell.live(column, this.timestamp, value, path):BufferCell.expiring(column, this.timestamp, this.ttl, this.nowInSec, value, path);
      this.builder.addCell(cell);
   }

   public void addCounter(ColumnMetadata column, long increment) throws InvalidRequestException {
      assert this.ttl == 0;

      this.builder.addCell(BufferCell.live(column, this.timestamp, CounterContext.instance().createUpdate(increment)));
   }

   public void setComplexDeletionTime(ColumnMetadata column) {
      this.builder.addComplexDeletion(column, this.deletionTime);
   }

   public void setComplexDeletionTimeForOverwrite(ColumnMetadata column) {
      this.builder.addComplexDeletion(column, new DeletionTime(this.deletionTime.markedForDeleteAt() - 1L, this.deletionTime.localDeletionTime()));
   }

   public Row buildRow() {
      Row built = this.builder.build();
      this.builder = null;
      return built;
   }

   public DeletionTime deletionTime() {
      return this.deletionTime;
   }

   public RangeTombstone makeRangeTombstone(ClusteringComparator comparator, Clustering clustering) {
      return this.makeRangeTombstone(Slice.make(comparator, new Object[]{clustering}));
   }

   public RangeTombstone makeRangeTombstone(Slice slice) {
      return new RangeTombstone(slice, this.deletionTime);
   }

   public Row getPrefetchedRow(DecoratedKey key, Clustering clustering) {
      if(this.prefetchedRows.isEmpty()) {
         return null;
      } else {
         Partition partition = (Partition)this.prefetchedRows.get(key);
         Row prefetchedRow = partition == null?null:(Row)partition.searchIterator(ColumnFilter.selection(partition.columns()), false).next(clustering);
         Row pendingMutations = this.builder.copy().build();
         return pendingMutations.isEmpty()?prefetchedRow:(prefetchedRow == null?pendingMutations:Rows.merge(prefetchedRow, pendingMutations, this.nowInSec).purge(DeletionPurger.PURGE_ALL, this.nowInSec, this.metadata.enforceStrictLiveness()));
      }
   }
}
