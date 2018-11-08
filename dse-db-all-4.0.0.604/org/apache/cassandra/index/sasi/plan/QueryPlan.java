package org.apache.cassandra.index.sasi.plan;

import io.reactivex.functions.Action;
import io.reactivex.functions.Function;
import io.reactivex.functions.Predicate;
import java.io.Closeable;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.flow.Threads;

public class QueryPlan {
   private final QueryController controller;

   public QueryPlan(ColumnFamilyStore cfs, ReadCommand command, long executionQuotaMs) {
      this.controller = new QueryController(cfs, (PartitionRangeReadCommand)command, executionQuotaMs);
   }

   private Operation analyze() {
      try {
         Operation.TreeBuilder tree = new Operation.TreeBuilder(this.controller);
         tree.add(this.controller.getExpressions());
         return tree.complete();
      } catch (Error | Exception var2) {
         this.controller.finish();
         throw var2;
      }
   }

   public Flow<FlowableUnfilteredPartition> execute(ReadExecutionController executionController) throws RequestTimeoutException {
      return (new QueryPlan.ResultRetriever(this.analyze(), this.controller, executionController)).getPartitions();
   }

   private static class ResultRetriever implements Function<DecoratedKey, Flow<FlowableUnfilteredPartition>> {
      private final AbstractBounds<PartitionPosition> keyRange;
      private final Operation operationTree;
      private final QueryController controller;
      private final ReadExecutionController executionController;

      public ResultRetriever(Operation operationTree, QueryController controller, ReadExecutionController executionController) {
         this.keyRange = controller.dataRange().keyRange();
         this.operationTree = operationTree;
         this.controller = controller;
         this.executionController = executionController;
      }

      public Flow<FlowableUnfilteredPartition> getPartitions() {
         if(this.operationTree == null) {
            return Flow.empty();
         } else {
            this.operationTree.skipTo((Long)((PartitionPosition)this.keyRange.left).getToken().getTokenValue());
            Flow<DecoratedKey> keys = Flow.fromIterable(() -> {
               return this.operationTree;
            }).lift(Threads.requestOnIo(TPCTaskType.READ_SECONDARY_INDEX)).flatMap(Flow::fromIterable);
            if(!((PartitionPosition)this.keyRange.right).isMinimum()) {
               keys = keys.takeWhile((key) -> {
                  return ((PartitionPosition)this.keyRange.right).compareTo(key) >= 0;
               });
            }

            if(!this.keyRange.inclusiveLeft()) {
               keys = keys.skippingMap((key) -> {
                  return key.compareTo((PartitionPosition)this.keyRange.left) == 0?null:key;
               });
            }

            return keys.flatMap(this).doOnClose(this::close);
         }
      }

      public Flow<FlowableUnfilteredPartition> apply(DecoratedKey key) {
         Flow<FlowableUnfilteredPartition> fp = this.controller.getPartition(key, this.executionController);
         return fp.map((partition) -> {
            Row staticRow = partition.staticRow();
            return partition.filterContent((row) -> {
               return this.operationTree.satisfiedBy(row, staticRow, true);
            });
         });
      }

      public void close() {
         FileUtils.closeQuietly((Closeable)this.operationTree);
         this.controller.finish();
      }
   }
}
