package org.apache.cassandra.index.internal.composites;

import io.reactivex.Completable;
import io.reactivex.functions.Function;
import java.nio.ByteBuffer;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.internal.CassandraIndexSearcher;
import org.apache.cassandra.index.internal.IndexEntry;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.flow.Flow;

public class StaticColumnsSearcher extends CassandraIndexSearcher {
   public StaticColumnsSearcher(ReadCommand command, RowFilter.Expression expression, CassandraIndex index) {
      super(command, expression, index);

      assert index.getIndexedColumn().isStatic();

   }

   protected Flow<FlowableUnfilteredPartition> queryDataFromIndex(DecoratedKey indexKey, FlowablePartition indexHits, ReadCommand command, ReadExecutionController executionController) {
      assert indexHits.staticRow() == Rows.EMPTY_STATIC_ROW;

      return indexHits.content().flatMap((hit) -> {
         IndexEntry nextEntry = this.index.decodeEntry(indexKey, hit);
         DecoratedKey partitionKey = this.index.baseCfs.decorateKey(nextEntry.indexedKey);
         if(command.selectsKey(partitionKey) && command.selectsClustering(partitionKey, nextEntry.indexedEntryClustering)) {
            SinglePartitionReadCommand dataCmd = SinglePartitionReadCommand.createForIndex(this.index.baseCfs.metadata(), command.nowInSec(), command.columnFilter(), RowFilter.NONE, DataLimits.NONE, partitionKey, command.clusteringIndexFilter(partitionKey));
            Flow<FlowableUnfilteredPartition> partition = dataCmd.queryStorage(this.index.baseCfs, executionController);
            return partition.skippingMap((p) -> {
               return this.filterStaleEntry(p, indexKey.getKey(), nextEntry, executionController.writeOpOrderGroup(), command.nowInSec());
            });
         } else {
            return Flow.empty();
         }
      });
   }

   private FlowableUnfilteredPartition filterStaleEntry(FlowableUnfilteredPartition dataIter, ByteBuffer indexValue, IndexEntry entry, OpOrder.Group writeOp, int nowInSec) throws Exception {
      boolean stale = false;
      if(!dataIter.header().partitionLevelDeletion.isLive()) {
         DeletionTime deletion = dataIter.header().partitionLevelDeletion;
         if(deletion.deletes(entry.timestamp)) {
            stale = true;
         }
      }

      if(!stale && !this.index.isStale(dataIter.staticRow(), indexValue, nowInSec)) {
         return dataIter;
      } else {
         ((Completable)deleteDecorator.apply(this.index.deleteStaleEntry(entry.indexValue, entry.indexClustering, new DeletionTime(entry.timestamp, nowInSec), writeOp))).subscribe();
         return null;
      }
   }
}
