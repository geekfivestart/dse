package org.apache.cassandra.index.internal.composites;

import io.reactivex.Completable;
import io.reactivex.functions.Function;
import io.reactivex.functions.Predicate;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.internal.CassandraIndexSearcher;
import org.apache.cassandra.index.internal.IndexEntry;
import org.apache.cassandra.utils.btree.BTreeSet;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.flow.GroupOp;

public class CompositesSearcher extends CassandraIndexSearcher {
   public CompositesSearcher(ReadCommand command, RowFilter.Expression expression, CassandraIndex index) {
      super(command, expression, index);

      assert !index.getIndexedColumn().isStatic();

   }

   private IndexEntry decodeMatchingEntry(DecoratedKey indexKey, Row hit, ReadCommand command) {
      IndexEntry entry = this.index.decodeEntry(indexKey, hit);
      DecoratedKey partitionKey = command.metadata().partitioner.decorateKey(entry.indexedKey);
      return command.selectsKey(partitionKey) && command.selectsClustering(partitionKey, entry.indexedEntryClustering)?entry:null;
   }

   protected Flow<FlowableUnfilteredPartition> queryDataFromIndex(final DecoratedKey indexKey, FlowablePartition indexHits, final ReadCommand command, final ReadExecutionController executionController) {
      assert indexHits.staticRow() == Rows.EMPTY_STATIC_ROW;

      class Collector implements GroupOp<IndexEntry, Flow<FlowableUnfilteredPartition>> {
         Collector() {
         }

         public boolean inSameGroup(IndexEntry l, IndexEntry r) {
            return l.indexedKey.equals(r.indexedKey);
         }

         public Flow<FlowableUnfilteredPartition> map(List<IndexEntry> entries) {
            DecoratedKey partitionKey = CompositesSearcher.this.index.baseCfs.decorateKey(((IndexEntry)entries.get(0)).indexedKey);
            BTreeSet.Builder clusterings = BTreeSet.builder(CompositesSearcher.this.index.baseCfs.getComparator());
            Iterator var4 = entries.iterator();

            while(var4.hasNext()) {
               IndexEntry e = (IndexEntry)var4.next();
               clusterings.add(e.indexedEntryClustering);
            }

            ClusteringIndexNamesFilter filter = new ClusteringIndexNamesFilter(clusterings.build(), false);
            SinglePartitionReadCommand dataCmd = SinglePartitionReadCommand.createForIndex(CompositesSearcher.this.index.baseCfs.metadata(), command.nowInSec(), command.columnFilter(), command.rowFilter(), DataLimits.NONE, partitionKey, filter);
            Flow<FlowableUnfilteredPartition> partition = dataCmd.queryStorage(CompositesSearcher.this.index.baseCfs, executionController);
            return partition.map((p) -> {
               return CompositesSearcher.this.filterStaleEntries(p, indexKey.getKey(), entries, executionController.writeOpOrderGroup(), command.nowInSec());
            });
         }
      }

      return indexHits.content().skippingMap((hit) -> {
         return this.decodeMatchingEntry(indexKey, hit, command);
      }).group(new Collector()).flatMap((x) -> {
         return x;
      });
   }

   private FlowableUnfilteredPartition filterStaleEntries(FlowableUnfilteredPartition dataIter, final ByteBuffer indexValue, final List<IndexEntry> entries, final OpOrder.Group writeOp, final int nowInSec) {
      final List<IndexEntry> staleEntries = new ArrayList();
      if(!dataIter.header().partitionLevelDeletion.isLive()) {
         DeletionTime deletion = dataIter.header().partitionLevelDeletion;
         entries.forEach((e) -> {
            if(deletion.deletes(e.timestamp)) {
               staleEntries.add(e);
            }

         });
      }

      final ClusteringComparator comparator = dataIter.header().metadata.comparator;
      class TransformedPartition extends FlowableUnfilteredPartition.Filter {
         private int entriesIdx;

         public TransformedPartition(FlowableUnfilteredPartition source) {
            super(source.content(), source.staticRow(), source.header(), (Predicate)null);
         }

         public boolean test(Unfiltered unfiltered) {
            if(!unfiltered.isRow()) {
               return true;
            } else {
               Row row = (Row)unfiltered;
               IndexEntry entry = this.findEntry(row.clustering());
               if(!CompositesSearcher.this.index.isStale(row, indexValue, nowInSec)) {
                  return true;
               } else {
                  staleEntries.add(entry);
                  return false;
               }
            }
         }

         private IndexEntry findEntry(Clustering clustering) {
            assert this.entriesIdx < entries.size() : "" + this.entriesIdx + ">=" + entries.size();

            while(this.entriesIdx < entries.size()) {
               IndexEntry entry = (IndexEntry)entries.get(this.entriesIdx++);
               int cmp = comparator.compare(entry.indexedEntryClustering, clustering);

               assert cmp <= 0;

               if(cmp == 0) {
                  return entry;
               }

               staleEntries.add(entry);
            }

            throw new AssertionError();
         }

         public void onFinal(Unfiltered next) {
            super.onFinal(next);
            ((Completable)CompositesSearcher.deleteDecorator.apply(CompositesSearcher.this.deleteAllEntries(staleEntries, writeOp, nowInSec))).subscribe();
         }

         public void onComplete() {
            super.onComplete();
            ((Completable)CompositesSearcher.deleteDecorator.apply(CompositesSearcher.this.deleteAllEntries(staleEntries, writeOp, nowInSec))).subscribe();
         }
      }

      return new TransformedPartition(dataIter);
   }

   private Completable deleteAllEntries(List<IndexEntry> entries, OpOrder.Group writeOp, int nowInSec) {
      return Completable.concat((Iterable)entries.stream().map((entry) -> {
         return this.index.deleteStaleEntry(entry.indexValue, entry.indexClustering, new DeletionTime(entry.timestamp, nowInSec), writeOp);
      }).collect(Collectors.toList()));
   }
}
