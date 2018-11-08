package org.apache.cassandra.index;

import io.reactivex.Completable;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.internal.CollatedViewIndexBuilder;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ReducingKeyIterator;
import org.apache.cassandra.io.sstable.format.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.flow.Flow;

public interface Index {
   Index.CollatedViewIndexBuildingSupport INDEX_BUILDER_SUPPORT = new Index.CollatedViewIndexBuildingSupport();

   default Index.IndexBuildingSupport getBuildTaskSupport() {
      return INDEX_BUILDER_SUPPORT;
   }

   Callable<?> getInitializationTask();

   IndexMetadata getIndexMetadata();

   Callable<?> getMetadataReloadTask(IndexMetadata var1);

   void register(IndexRegistry var1);

   Optional<ColumnFamilyStore> getBackingTable();

   Callable<?> getBlockingFlushTask();

   Callable<?> getInvalidateTask();

   Callable<?> getTruncateTask(long var1);

   default Callable<?> getPreJoinTask(boolean hadBootstrap) {
      return null;
   }

   boolean shouldBuildBlocking();

   default SSTableFlushObserver getFlushObserver(Descriptor descriptor, OperationType opType) {
      return null;
   }

   boolean dependsOn(ColumnMetadata var1);

   boolean supportsExpression(ColumnMetadata var1, Operator var2);

   AbstractType<?> customExpressionValueType();

   RowFilter getPostIndexQueryFilter(RowFilter var1);

   long getEstimatedResultRows();

   void validate(PartitionUpdate var1) throws InvalidRequestException;

   Index.Indexer indexerFor(DecoratedKey var1, RegularAndStaticColumns var2, int var3, OpOrder.Group var4, IndexTransaction.Type var5);

   default void validate(ReadCommand command) throws InvalidRequestException {
   }

   BiFunction<Flow<FlowablePartition>, ReadCommand, Flow<FlowablePartition>> postProcessorFor(ReadCommand var1);

   Index.Searcher searcherFor(ReadCommand var1);

   public interface Searcher {
      Flow<FlowableUnfilteredPartition> search(ReadExecutionController var1);
   }

   public interface Indexer {
      void begin();

      Completable partitionDelete(DeletionTime var1);

      Completable rangeTombstone(RangeTombstone var1);

      Completable insertRow(Row var1);

      Completable updateRow(Row var1, Row var2);

      Completable removeRow(Row var1);

      Completable finish();
   }

   public static class CollatedViewIndexBuildingSupport implements Index.IndexBuildingSupport {
      public CollatedViewIndexBuildingSupport() {
      }

      public SecondaryIndexBuilder getIndexBuildTask(ColumnFamilyStore cfs, Set<Index> indexes, Collection<SSTableReader> sstables) {
         return new CollatedViewIndexBuilder(cfs, indexes, new ReducingKeyIterator(sstables));
      }
   }

   public interface IndexBuildingSupport {
      SecondaryIndexBuilder getIndexBuildTask(ColumnFamilyStore var1, Set<Index> var2, Collection<SSTableReader> var3);
   }
}
