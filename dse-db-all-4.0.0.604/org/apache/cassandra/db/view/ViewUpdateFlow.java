package org.apache.cassandra.db.view;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import io.reactivex.Single;
import io.reactivex.functions.Action;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.cassandra.db.Clusterable;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.ArrayBackedRow;
import org.apache.cassandra.db.rows.FlowablePartitions;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.btree.BTreeSet;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.flow.FlowTransform;

public class ViewUpdateFlow {
   private static Function<PartitionUpdate, DecoratedKey> byKey = (p) -> {
      return p.partitionKey();
   };
   private static Function<PartitionUpdate, TableId> byCf = (p) -> {
      return p.metadata().id;
   };

   public ViewUpdateFlow() {
   }

   public static Single<Collection<Mutation>> forUpdate(PartitionUpdate update, TableMetadataRef basetableMetadataRef, Collection<View> views, int nowInSec) {
      return createViewUpdateFlow(createBaseTableUpdateFlow(update, basetableMetadataRef, views, nowInSec), update.partitionKey(), views, nowInSec);
   }

   public static Single<Collection<Mutation>> forRebuild(UnfilteredRowIterator update, TableMetadataRef basetableMetadataRef, Collection<View> views, int nowInSec) {
      return createViewUpdateFlow(createBaseTableUpdateFlowNoExisting(update, basetableMetadataRef, nowInSec), update.partitionKey(), views, nowInSec);
   }

   private static Single<Collection<Mutation>> createViewUpdateFlow(Flow<ViewUpdateFlow.RowUpdate> baseTableUpdateFlow, DecoratedKey partitionKey, Collection<View> views, int nowInSec) {
      List<ViewUpdateGenerator> generators = new ArrayList(views.size());
      Iterator var5 = views.iterator();

      while(var5.hasNext()) {
         View view = (View)var5.next();
         generators.add(new ViewUpdateGenerator(view, partitionKey, nowInSec));
      }

      return baseTableUpdateFlow.flatMap((affectedRow) -> {
         return createViewUpdateFlow(generators, affectedRow);
      }).toList().mapToRxSingle((viewUpdates) -> {
         return createMutations(viewUpdates);
      });
   }

   private static Flow<ViewUpdateFlow.RowUpdate> createBaseTableUpdateFlow(PartitionUpdate update, TableMetadataRef basetableMetadataRef, Collection<View> views, int nowInSec) {
      SinglePartitionReadCommand command = readExistingRowsCommand(update, views, nowInSec);
      long start = System.nanoTime();
      Flow<ViewUpdateFlow.RowUpdate> existingAndMerged = command == null?Flow.empty():command.executeLocally().flatMap((fup) -> {
         return new ViewUpdateFlow.BaseTableUpdateFlow(update.unfilteredIterator(), fup, basetableMetadataRef, nowInSec);
      });
      return existingAndMerged.doOnClose(() -> {
         Keyspace.openAndGetStore(update.metadata()).metric.viewReadTime.update(System.nanoTime() - start, TimeUnit.NANOSECONDS);
      });
   }

   private static Flow<ViewUpdateFlow.RowUpdate> createBaseTableUpdateFlowNoExisting(UnfilteredRowIterator update, TableMetadataRef basetableMetadataRef, int nowInSec) {
      FlowableUnfilteredPartition emptyFup = FlowablePartitions.empty(basetableMetadataRef.get(), update.partitionKey(), false);
      return new ViewUpdateFlow.BaseTableUpdateFlow(update, emptyFup, basetableMetadataRef, nowInSec);
   }

   public static Flow<PartitionUpdate> createViewUpdateFlow(List<ViewUpdateGenerator> generators, ViewUpdateFlow.RowUpdate update) {
      assert !generators.isEmpty();

      return generators.size() == 1?((ViewUpdateGenerator)generators.get(0)).createViewUpdates(update.before, update.after):Flow.fromIterable(generators).flatMap((generator) -> {
         return generator.createViewUpdates(update.before, update.after);
      });
   }

   private static SinglePartitionReadCommand readExistingRowsCommand(PartitionUpdate updates, Collection<View> views, int nowInSec) {
      Slices.Builder sliceBuilder = null;
      DeletionInfo deletionInfo = updates.deletionInfo();
      TableMetadata metadata = updates.metadata();
      DecoratedKey key = updates.partitionKey();
      if(!deletionInfo.isLive()) {
         sliceBuilder = new Slices.Builder(metadata.comparator);
         Iterator iter;
         if(!deletionInfo.getPartitionDeletion().isLive()) {
            iter = views.iterator();

            while(iter.hasNext()) {
               View view = (View)iter.next();
               sliceBuilder.addAll(view.getSelectStatement().clusteringIndexFilterAsSlices());
            }
         } else {
            assert deletionInfo.hasRanges();

            iter = deletionInfo.rangeIterator(false);

            while(iter.hasNext()) {
               sliceBuilder.add(((RangeTombstone)iter.next()).deletedSlice());
            }
         }
      }

      BTreeSet.Builder<Clustering> namesBuilder = sliceBuilder == null?BTreeSet.builder(metadata.comparator):null;
      Iterator var13 = updates.iterator();

      while(var13.hasNext()) {
         Row row = (Row)var13.next();
         if(affectsAnyViews(key, row, views)) {
            if(namesBuilder == null) {
               sliceBuilder.add(Slice.make(row.clustering()));
            } else {
               namesBuilder.add(row.clustering());
            }
         }
      }

      NavigableSet<Clustering> names = namesBuilder == null?null:namesBuilder.build();
      if(names != null && names.isEmpty()) {
         return null;
      } else {
         ClusteringIndexFilter clusteringFilter = names == null?new ClusteringIndexSliceFilter(sliceBuilder.build(), false):new ClusteringIndexNamesFilter(names, false);
         ColumnFilter queriedColumns = views.size() == 1 && metadata.enforceStrictLiveness()?((View)Iterables.getOnlyElement(views)).getSelectStatement().queriedColumns():ColumnFilter.all(metadata);
         RowFilter rowFilter = RowFilter.NONE;
         return SinglePartitionReadCommand.create(metadata, nowInSec, queriedColumns, rowFilter, DataLimits.NONE, key, (ClusteringIndexFilter)clusteringFilter);
      }
   }

   private static boolean affectsAnyViews(DecoratedKey partitionKey, Row update, Collection<View> views) {
      Iterator var3 = views.iterator();

      View view;
      do {
         if(!var3.hasNext()) {
            return false;
         }

         view = (View)var3.next();
      } while(!view.mayBeAffectedBy(partitionKey, update));

      return true;
   }

   private static Collection<Mutation> createMutations(List<PartitionUpdate> updates) {
      if(updates.isEmpty()) {
         return Collections.emptyList();
      } else {
         String keyspaceName = ((PartitionUpdate)updates.get(0)).metadata().keyspace;
         Map<DecoratedKey, List<PartitionUpdate>> updatesByKey = (Map)updates.stream().collect(Collectors.groupingBy(byKey));
         List<Mutation> mutations = new ArrayList();
         Iterator var4 = updatesByKey.entrySet().iterator();

         while(var4.hasNext()) {
            Entry<DecoratedKey, List<PartitionUpdate>> updatesWithSameKey = (Entry)var4.next();
            DecoratedKey key = (DecoratedKey)updatesWithSameKey.getKey();
            Mutation mutation = new Mutation(keyspaceName, key);
            Map<TableId, List<PartitionUpdate>> updatesByCf = (Map)((List)updatesWithSameKey.getValue()).stream().collect(Collectors.groupingBy(byCf));
            Iterator var9 = updatesByCf.values().iterator();

            while(var9.hasNext()) {
               List<PartitionUpdate> updatesToMerge = (List)var9.next();
               mutation.add(PartitionUpdate.merge(updatesToMerge));
            }

            mutations.add(mutation);
         }

         return mutations;
      }
   }

   private static Row emptyRow(Clustering clustering, DeletionTime deletion) {
      return deletion.isLive()?null:ArrayBackedRow.emptyDeletedRow(clustering, Row.Deletion.regular(deletion));
   }

   public static class RowUpdate {
      public final Row before;
      public final Row after;

      private RowUpdate(Row before, Row after) {
         this.before = before;
         this.after = after;
      }

      public static ViewUpdateFlow.RowUpdate create(Row existingBaseRow, Row updateBaseRow, int nowInSec) {
         assert !updateBaseRow.isEmpty();

         Row mergedBaseRow = existingBaseRow == null?updateBaseRow:Rows.merge(existingBaseRow, updateBaseRow, nowInSec);
         return new ViewUpdateFlow.RowUpdate(existingBaseRow, mergedBaseRow);
      }
   }

   private static class DeletionTracker {
      private final DeletionTime partitionDeletion;
      private DeletionTime deletion;

      public DeletionTracker(DeletionTime partitionDeletion) {
         this.partitionDeletion = partitionDeletion;
         this.deletion = partitionDeletion;
      }

      public void update(Unfiltered marker) {
         assert marker instanceof RangeTombstoneMarker;

         RangeTombstoneMarker rtm = (RangeTombstoneMarker)marker;
         this.deletion = rtm.isOpen(false)?rtm.openDeletionTime(false):this.partitionDeletion;
      }

      public DeletionTime currentDeletion() {
         return this.deletion;
      }
   }

   static class BaseTableUpdateFlow extends FlowTransform<Unfiltered, ViewUpdateFlow.RowUpdate> {
      private final TableMetadataRef baseTableMetadataRef;
      private final UnfilteredRowIterator updates;
      private final PeekingIterator<Unfiltered> updatesIter;
      private final ViewUpdateFlow.DeletionTracker existingsDeletion;
      private final ViewUpdateFlow.DeletionTracker updatesDeletion;
      private int nowInSec;
      private Unfiltered cachedExisting;
      private boolean finishedExistings;

      private BaseTableUpdateFlow(UnfilteredRowIterator updates, FlowableUnfilteredPartition existings, TableMetadataRef tableMetadataRef, int nowInSec) {
         super(existings.content());
         this.cachedExisting = null;
         this.finishedExistings = false;
         this.baseTableMetadataRef = tableMetadataRef;
         this.existingsDeletion = new ViewUpdateFlow.DeletionTracker(existings.partitionLevelDeletion());
         this.updatesDeletion = new ViewUpdateFlow.DeletionTracker(updates.partitionLevelDeletion());
         this.nowInSec = nowInSec;
         this.updates = updates;
         this.updatesIter = Iterators.peekingIterator(updates);
      }

      public void requestNext() {
         this.processNext();
      }

      private void processNext() {
         ViewUpdateFlow.RowUpdate next = this.peekExisting() == null?null:this.getNext();
         if(next == null && !this.finishedExistings) {
            this.source.requestNext();
         } else if(next == null && this.finishedExistings) {
            this.complete();
         } else {
            this.subscriber.onNext(next);
         }

      }

      public void onNext(Unfiltered item) {
         this.cachedExisting = item;
         this.processNext();
      }

      public void onFinal(Unfiltered item) {
         this.finishedExistings = true;
         this.cachedExisting = item;
         this.processNext();
      }

      public void onComplete() {
         this.finishedExistings = true;
         this.processNext();
      }

      private void complete() {
         assert this.peekExisting() == null && this.finishedExistings;

         Unfiltered update;
         do {
            if(!this.updatesIter.hasNext()) {
               this.subscriber.onComplete();
               return;
            }

            update = (Unfiltered)this.updatesIter.next();
         } while(update.isRangeTombstoneMarker());

         Row updateRow = (Row)update;
         this.subscriber.onNext(ViewUpdateFlow.RowUpdate.create(ViewUpdateFlow.emptyRow(updateRow.clustering(), this.existingsDeletion.currentDeletion()), updateRow, this.nowInSec));
      }

      private ViewUpdateFlow.RowUpdate getNext() {
         assert this.peekExisting() != null;

         Row existingRow;
         Row updateRow;
         while(true) {
            Unfiltered existing;
            if(!this.updatesIter.hasNext()) {
               existing = this.consumeExisting();
               if(!this.updatesDeletion.currentDeletion().isLive() && !existing.isRangeTombstoneMarker()) {
                  existingRow = (Row)existing;
                  return ViewUpdateFlow.RowUpdate.create(existingRow, ViewUpdateFlow.emptyRow(existingRow.clustering(), this.updatesDeletion.currentDeletion()), this.nowInSec);
               }

               return null;
            }

            existing = this.peekExisting();
            Unfiltered update = (Unfiltered)this.updatesIter.peek();
            int cmp = this.baseTableMetadataRef.get().comparator.compare((Clusterable)update, (Clusterable)existing);
            if(cmp < 0) {
               if(update.isRangeTombstoneMarker()) {
                  this.updatesDeletion.update((Unfiltered)this.updatesIter.next());
                  continue;
               }

               updateRow = ((Row)this.updatesIter.next()).withRowDeletion(this.updatesDeletion.currentDeletion());
               existingRow = ViewUpdateFlow.emptyRow(updateRow.clustering(), this.existingsDeletion.currentDeletion());
               break;
            }

            if(cmp > 0) {
               Unfiltered nextExisting = this.consumeExisting();
               if(existing.isRangeTombstoneMarker()) {
                  this.existingsDeletion.update(nextExisting);
                  return null;
               }

               existingRow = ((Row)nextExisting).withRowDeletion(this.existingsDeletion.currentDeletion());
               updateRow = ViewUpdateFlow.emptyRow(existingRow.clustering(), this.updatesDeletion.currentDeletion());
               if(updateRow == null) {
                  return null;
               }
            } else {
               if(update.isRangeTombstoneMarker()) {
                  assert existing.isRangeTombstoneMarker();

                  this.updatesDeletion.update((Unfiltered)this.updatesIter.next());
                  this.existingsDeletion.update(this.consumeExisting());
                  return null;
               }

               assert !existing.isRangeTombstoneMarker();

               existingRow = ((Row)this.consumeExisting()).withRowDeletion(this.existingsDeletion.currentDeletion());
               updateRow = ((Row)this.updatesIter.next()).withRowDeletion(this.updatesDeletion.currentDeletion());
            }
            break;
         }

         return ViewUpdateFlow.RowUpdate.create(existingRow, updateRow, this.nowInSec);
      }

      private Unfiltered peekExisting() {
         return this.cachedExisting;
      }

      private Unfiltered consumeExisting() {
         Unfiltered toReturn = this.cachedExisting;
         this.cachedExisting = null;
         return toReturn;
      }

      public void close() throws Exception {
         this.updates.close();
      }
   }
}
