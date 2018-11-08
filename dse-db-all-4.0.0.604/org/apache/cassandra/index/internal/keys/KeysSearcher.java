package org.apache.cassandra.index.internal.keys;

import io.reactivex.Completable;
import io.reactivex.functions.Function;
import java.nio.ByteBuffer;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.internal.CassandraIndexSearcher;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.flow.Flow;

public class KeysSearcher extends CassandraIndexSearcher {
   public KeysSearcher(ReadCommand command, RowFilter.Expression expression, CassandraIndex indexer) {
      super(command, expression, indexer);
   }

   protected Flow<FlowableUnfilteredPartition> queryDataFromIndex(DecoratedKey indexKey, FlowablePartition indexHits, ReadCommand command, ReadExecutionController executionController) {
      assert indexHits.staticRow() == Rows.EMPTY_STATIC_ROW;

      return indexHits.content().flatMap((hit) -> {
         DecoratedKey key = this.index.baseCfs.decorateKey(hit.clustering().get(0));
         if(!command.selectsKey(key)) {
            return Flow.empty();
         } else {
            ColumnFilter extendedFilter = this.getExtendedFilter(command.columnFilter());
            SinglePartitionReadCommand dataCmd = SinglePartitionReadCommand.createForIndex(this.index.baseCfs.metadata(), command.nowInSec(), extendedFilter, command.rowFilter(), DataLimits.NONE, key, command.clusteringIndexFilter(key));
            Flow<FlowableUnfilteredPartition> partition = dataCmd.queryStorage(this.index.baseCfs, executionController);
            return partition.skippingMap((p) -> {
               return this.filterIfStale(p, hit, indexKey.getKey(), executionController.writeOpOrderGroup(), command.nowInSec());
            });
         }
      });
   }

   private ColumnFilter getExtendedFilter(ColumnFilter initialFilter) {
      if(this.command.columnFilter().fetches(this.index.getIndexedColumn())) {
         return initialFilter;
      } else {
         ColumnFilter.Builder builder = ColumnFilter.selectionBuilder();
         builder.addAll(initialFilter.fetchedColumns());
         builder.add(this.index.getIndexedColumn());
         return builder.build();
      }
   }

   private FlowableUnfilteredPartition filterIfStale(FlowableUnfilteredPartition partition, Row indexHit, ByteBuffer indexedValue, OpOrder.Group writeOp, int nowInSec) throws Exception {
      Row data = partition.staticRow();
      if(!this.index.isStale(data, indexedValue, nowInSec)) {
         return partition;
      } else {
         ((Completable)deleteDecorator.apply(this.index.deleteStaleEntry(this.index.getIndexCfs().decorateKey(indexedValue), this.makeIndexClustering(partition.header().partitionKey.getKey(), Clustering.EMPTY), new DeletionTime(indexHit.primaryKeyLivenessInfo().timestamp(), nowInSec), writeOp))).subscribe();
         return null;
      }
   }
}
