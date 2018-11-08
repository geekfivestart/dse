package org.apache.cassandra.index.internal;

import com.google.common.annotations.VisibleForTesting;
import io.reactivex.Completable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.function.Function;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.db.rows.FlowablePartitions;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.btree.BTreeSet;
import org.apache.cassandra.utils.flow.Flow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CassandraIndexSearcher implements Index.Searcher {
   private static final Logger logger = LoggerFactory.getLogger(CassandraIndexSearcher.class);
   private static final Function<Completable, Completable> defaultDecorator = (x) -> {
      return x;
   };
   protected static Function<Completable, Completable> deleteDecorator;
   private final RowFilter.Expression expression;
   protected final CassandraIndex index;
   protected final ReadCommand command;

   public CassandraIndexSearcher(ReadCommand command, RowFilter.Expression expression, CassandraIndex index) {
      this.command = command;
      this.expression = expression;
      this.index = index;
   }

   public Flow<FlowableUnfilteredPartition> search(ReadExecutionController executionController) {
      DecoratedKey indexKey = ((ColumnFamilyStore)this.index.getBackingTable().get()).decorateKey(this.expression.getIndexValue());
      return this.queryIndex(indexKey, this.command, executionController).flatMap((i) -> {
         return this.queryDataFromIndex(indexKey, FlowablePartitions.filter(i, this.command.nowInSec()), this.command, executionController);
      });
   }

   private Flow<FlowableUnfilteredPartition> queryIndex(DecoratedKey indexKey, ReadCommand command, ReadExecutionController executionController) {
      ClusteringIndexFilter filter = this.makeIndexFilter(command);
      ColumnFamilyStore indexCfs = (ColumnFamilyStore)this.index.getBackingTable().get();
      TableMetadata indexMetadata = indexCfs.metadata();
      return SinglePartitionReadCommand.createForIndex(indexMetadata, command.nowInSec(), ColumnFilter.all(indexMetadata), RowFilter.NONE, DataLimits.NONE, indexKey, filter).queryStorage(indexCfs, executionController.indexReadController());
   }

   private ClusteringIndexFilter makeIndexFilter(ReadCommand command) {
      if(command instanceof SinglePartitionReadCommand) {
         SinglePartitionReadCommand sprc = (SinglePartitionReadCommand)command;
         ByteBuffer pk = sprc.partitionKey().getKey();
         ClusteringIndexFilter filter = sprc.clusteringIndexFilter();
         Iterator var20;
         if(filter instanceof ClusteringIndexNamesFilter) {
            NavigableSet<Clustering> requested = ((ClusteringIndexNamesFilter)filter).requestedRows();
            BTreeSet.Builder<Clustering> clusterings = BTreeSet.builder(this.index.getIndexComparator());
            var20 = requested.iterator();

            while(var20.hasNext()) {
               Clustering c = (Clustering)var20.next();
               clusterings.add(this.makeIndexClustering(pk, c));
            }

            return new ClusteringIndexNamesFilter(clusterings.build(), filter.isReversed());
         } else {
            Slices requested = ((ClusteringIndexSliceFilter)filter).requestedSlices();
            Slices.Builder builder = new Slices.Builder(this.index.getIndexComparator());
            var20 = requested.iterator();

            while(var20.hasNext()) {
               Slice slice = (Slice)var20.next();
               builder.add(this.makeIndexBound(pk, slice.start()), this.makeIndexBound(pk, slice.end()));
            }

            return new ClusteringIndexSliceFilter(builder.build(), filter.isReversed());
         }
      } else {
         DataRange dataRange = ((PartitionRangeReadCommand)command).dataRange();
         AbstractBounds<PartitionPosition> range = dataRange.keyRange();
         Slice slice = Slice.ALL;
         if(range.left instanceof DecoratedKey) {
            if(range.right instanceof DecoratedKey) {
               DecoratedKey startKey = (DecoratedKey)range.left;
               DecoratedKey endKey = (DecoratedKey)range.right;
               ClusteringBound start = ClusteringBound.BOTTOM;
               ClusteringBound end = ClusteringBound.TOP;
               if(!dataRange.isNamesQuery() && !this.index.indexedColumn.isStatic()) {
                  ClusteringIndexSliceFilter startSliceFilter = (ClusteringIndexSliceFilter)dataRange.clusteringIndexFilter(startKey);
                  ClusteringIndexSliceFilter endSliceFilter = (ClusteringIndexSliceFilter)dataRange.clusteringIndexFilter(endKey);

                  assert !startSliceFilter.isReversed() && !endSliceFilter.isReversed();

                  Slices startSlices = startSliceFilter.requestedSlices();
                  Slices endSlices = endSliceFilter.requestedSlices();
                  if(startSlices.size() > 0) {
                     start = startSlices.get(0).start();
                  }

                  if(endSlices.size() > 0) {
                     end = endSlices.get(endSlices.size() - 1).end();
                  }
               }

               slice = Slice.make(this.makeIndexBound(startKey.getKey(), start), this.makeIndexBound(endKey.getKey(), end));
            } else {
               slice = Slice.make(this.makeIndexBound(((DecoratedKey)range.left).getKey(), ClusteringBound.BOTTOM), ClusteringBound.TOP);
            }
         }

         return new ClusteringIndexSliceFilter(Slices.with(this.index.getIndexComparator(), slice), false);
      }
   }

   private ClusteringBound makeIndexBound(ByteBuffer rowKey, ClusteringBound bound) {
      return this.index.buildIndexClusteringPrefix(rowKey, bound, (CellPath)null).buildBound(bound.isStart(), bound.isInclusive());
   }

   protected Clustering makeIndexClustering(ByteBuffer rowKey, Clustering clustering) {
      return this.index.buildIndexClusteringPrefix(rowKey, clustering, (CellPath)null).build();
   }

   protected abstract Flow<FlowableUnfilteredPartition> queryDataFromIndex(DecoratedKey var1, FlowablePartition var2, ReadCommand var3, ReadExecutionController var4);

   @VisibleForTesting
   public static void setDeleteDecorator(Function<Completable, Completable> newFn) {
      deleteDecorator = newFn;
   }

   @VisibleForTesting
   public static void resetDeleteDecorator() {
      deleteDecorator = defaultDecorator;
   }

   static {
      deleteDecorator = defaultDecorator;
   }
}
