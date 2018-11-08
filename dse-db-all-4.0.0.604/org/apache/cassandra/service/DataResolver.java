package org.apache.cassandra.service;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.reactivex.Completable;
import io.reactivex.CompletableOnSubscribe;
import io.reactivex.CompletableSource;
import io.reactivex.functions.Action;
import io.reactivex.functions.Function;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadContext;
import org.apache.cassandra.db.ReadReconciliationObserver;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.db.rows.FlowablePartitions;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowDiffListener;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.ThrottledUnfilteredIterator;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.ExcludingBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingVersion;
import org.apache.cassandra.net.Request;
import org.apache.cassandra.net.Response;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.flow.Flow;

public class DataResolver extends ResponseResolver<FlowablePartition> {
   DataResolver(ReadCommand command, ReadContext ctx, int maxResponseCount) {
      super(command, ctx, maxResponseCount);
   }

   public Flow<FlowablePartition> getData() {
      return this.fromSingleResponseFiltered((ReadResponse)((Response)this.responses.iterator().next()).payload());
   }

   public boolean isDataPresent() {
      return !this.responses.isEmpty();
   }

   public Completable compareResponses() {
      return FlowablePartitions.allRows(this.resolve()).processToRxCompletable();
   }

   public Flow<FlowablePartition> resolve() {
      int count = this.responses.size();
      List<Flow<FlowableUnfilteredPartition>> results = new ArrayList(count);
      InetAddress[] sources = new InetAddress[count];

      for(int i = 0; i < count; ++i) {
         Response<ReadResponse> msg = (Response)this.responses.get(i);
         results.add(((ReadResponse)msg.payload()).data(this.command));
         sources[i] = msg.from();
      }

      DataLimits.Counter mergedResultCounter = this.command.limits().newCounter(this.command.nowInSec(), true, this.command.selectsFullPartition(), this.command.metadata().enforceStrictLiveness());
      Flow<FlowableUnfilteredPartition> merged = this.mergeWithShortReadProtection(results, sources, mergedResultCounter);
      Flow<FlowablePartition> filtered = FlowablePartitions.filter(merged, this.command.nowInSec());
      Flow<FlowablePartition> counted = DataLimits.truncateFiltered(filtered, mergedResultCounter);
      return FlowablePartitions.skipEmptyPartitions(counted);
   }

   private Completable completeOnReadRepairAnswersReceived(ReadRepairFuture repairResults) {
      return Completable.create((subscriber) -> {
         repairResults.whenComplete((result, error) -> {
            if(error != null) {
               subscriber.onError(error);
            } else {
               subscriber.onComplete();
            }

         });
      }).timeout(DatabaseDescriptor.getWriteRpcTimeout(), TimeUnit.MILLISECONDS, (subscriber) -> {
         int required = this.ctx.requiredResponses();
         if(Tracing.isTracing()) {
            Tracing.trace("Timed out while read-repairing after receiving all {} data and digest responses", (Object)Integer.valueOf(required));
         } else {
            logger.debug("Timeout while read-repairing after receiving all {} data and digest responses", Integer.valueOf(required));
         }

         subscriber.onError(new ReadTimeoutException(this.consistency(), required - 1, required, true));
      });
   }

   private Flow<FlowableUnfilteredPartition> mergeWithShortReadProtection(List<Flow<FlowableUnfilteredPartition>> results, InetAddress[] sources, DataLimits.Counter mergedResultCounter) {
      if(results.size() == 1) {
         return (Flow)results.get(0);
      } else {
         if(!this.command.limits().isUnlimited()) {
            for(int i = 0; i < results.size(); ++i) {
               results.set(i, this.withShortReadProtection(sources[i], (Flow)results.get(i), mergedResultCounter));
            }
         }

         ReadRepairFuture repairResults = new ReadRepairFuture();
         FlowablePartitions.MergeListener listener = new DataResolver.RepairMergeListener(sources, repairResults);
         Flow var10000 = FlowablePartitions.mergePartitions(results, this.command.nowInSec(), listener);
         repairResults.getClass();
         return Flow.concat(var10000.doOnComplete(repairResults::onAllRepairSent), this.completeOnReadRepairAnswersReceived(repairResults));
      }
   }

   private Flow<FlowableUnfilteredPartition> withShortReadProtection(InetAddress source, Flow<FlowableUnfilteredPartition> data, DataLimits.Counter mergedResultCounter) {
      DataResolver.ShortReadResponseProtection shortReadProtection = new DataResolver.ShortReadResponseProtection(source, mergedResultCounter);
      return shortReadProtection.apply(data);
   }

   private static class RetryResolver extends ResponseResolver<FlowableUnfilteredPartition> {
      RetryResolver(ReadCommand command, ReadContext ctx) {
         super(command, ctx, 1);
      }

      public Flow<FlowableUnfilteredPartition> getData() {
         return this.fromSingleResponse((ReadResponse)((Response)this.responses.iterator().next()).payload());
      }

      public Flow<FlowableUnfilteredPartition> resolve() throws DigestMismatchException {
         return this.getData();
      }

      public Completable completeOnReadRepairAnswersReceived() {
         return Completable.complete();
      }

      public Completable compareResponses() throws DigestMismatchException {
         return Completable.complete();
      }

      public boolean isDataPresent() {
         return !this.responses.isEmpty();
      }
   }

   private class ShortReadResponseProtection {
      private final InetAddress source;
      private final DataLimits.Counter singleResultCounter;
      private final DataLimits.Counter mergedResultCounter;
      private volatile DecoratedKey lastPartitionKey;
      private volatile boolean partitionsFetched;

      private ShortReadResponseProtection(InetAddress source, DataLimits.Counter mergedResultCounter) {
         this.source = source;
         this.singleResultCounter = DataResolver.this.command.limits().newCounter(DataResolver.this.command.nowInSec(), false, DataResolver.this.command.selectsFullPartition(), DataResolver.this.command.metadata().enforceStrictLiveness());
         this.mergedResultCounter = mergedResultCounter;
      }

      private Flow<FlowableUnfilteredPartition> apply(Flow<FlowableUnfilteredPartition> data) {
         Flow<FlowableUnfilteredPartition> flow = DataLimits.countUnfilteredPartitions(data, this.singleResultCounter);
         if(!DataResolver.this.command.isLimitedToOnePartition()) {
            flow = flow.concatWith(this::moreContents);
         }

         Flow var10000 = flow.map(this::applyPartition);
         DataLimits.Counter var10001 = this.singleResultCounter;
         this.singleResultCounter.getClass();
         return var10000.doOnClose(var10001::endOfIteration);
      }

      private FlowableUnfilteredPartition applyPartition(FlowableUnfilteredPartition partition) {
         this.partitionsFetched = true;
         this.lastPartitionKey = partition.partitionKey();
         return (new DataResolver.ShortReadResponseProtection.ShortReadRowsProtection()).applyPartition(partition);
      }

      public Flow<FlowableUnfilteredPartition> moreContents() {
         assert !this.mergedResultCounter.isDone();

         assert !DataResolver.this.command.limits().isUnlimited();

         assert !DataResolver.this.command.isLimitedToOnePartition();

         if(!this.singleResultCounter.isDone() && DataResolver.this.command.limits().perPartitionCount() == 2147483647) {
            return null;
         } else if(!this.partitionsFetched) {
            return null;
         } else {
            this.partitionsFetched = false;
            int toQuery = DataResolver.this.command.limits().count() != 2147483647?DataResolver.this.command.limits().count() - this.counted(this.mergedResultCounter):DataResolver.this.command.limits().perPartitionCount();
            ColumnFamilyStore.metricsFor(DataResolver.this.command.metadata().id).shortReadProtectionRequests.mark();
            Tracing.trace("Requesting {} extra rows from {} for short read protection", Integer.valueOf(toQuery), this.source);
            PartitionRangeReadCommand cmd = this.makeFetchAdditionalPartitionReadCommand(toQuery);
            return DataLimits.countUnfilteredPartitions(this.executeReadCommand(cmd), this.singleResultCounter);
         }
      }

      private int counted(DataLimits.Counter counter) {
         return DataResolver.this.command.limits().isGroupByLimit()?counter.rowCounted():counter.counted();
      }

      private PartitionRangeReadCommand makeFetchAdditionalPartitionReadCommand(int toQuery) {
         PartitionRangeReadCommand cmd = (PartitionRangeReadCommand)DataResolver.this.command;
         DataLimits newLimits = cmd.limits().forShortReadRetry(toQuery);
         AbstractBounds<PartitionPosition> bounds = cmd.dataRange().keyRange();
         AbstractBounds<PartitionPosition> newBounds = bounds.inclusiveRight()?new Range(this.lastPartitionKey, bounds.right):new ExcludingBounds(this.lastPartitionKey, bounds.right);
         DataRange newDataRange = cmd.dataRange().forSubRange((AbstractBounds)newBounds);
         return cmd.withUpdatedLimitsAndDataRange(newLimits, newDataRange);
      }

      private Flow<FlowableUnfilteredPartition> executeReadCommand(ReadCommand cmd) {
         DataResolver.RetryResolver resolver = new DataResolver.RetryResolver(cmd, DataResolver.this.ctx.withConsistency(ConsistencyLevel.ONE).withObserver((ReadReconciliationObserver)null));
         ReadCallback<FlowableUnfilteredPartition> handler = ReadCallback.forResolver(resolver, Collections.singletonList(this.source));
         MessagingService.instance().send((Request)cmd.requestTo(this.source), handler);
         return handler.result();
      }

      private class ShortReadRowsProtection {
         private volatile Clustering lastClustering;
         private volatile int lastCounted;
         private volatile int lastFetched;
         private volatile int lastQueried;

         private ShortReadRowsProtection() {
            this.lastCounted = 0;
            this.lastFetched = 0;
            this.lastQueried = 0;
         }

         private FlowableUnfilteredPartition applyPartition(FlowableUnfilteredPartition partition) {
            Flow var10001 = partition.content().concatWith(this::moreContents).map(this::applyUnfiltered);
            DataLimits.Counter var10002 = ShortReadResponseProtection.this.singleResultCounter;
            var10002.getClass();
            return partition.withContent(var10001.doOnClose(var10002::endOfPartition));
         }

         private Unfiltered applyUnfiltered(Unfiltered unfiltered) {
            if(unfiltered instanceof Row) {
               this.lastClustering = ((Row)unfiltered).clustering();
            }

            return unfiltered;
         }

         private Flow<Unfiltered> moreContents() {
            assert !ShortReadResponseProtection.this.mergedResultCounter.isDoneForPartition();

            assert !DataResolver.this.command.limits().isUnlimited();

            if(!ShortReadResponseProtection.this.singleResultCounter.isDoneForPartition() && DataResolver.this.command.limits().perPartitionCount() == 2147483647) {
               return null;
            } else if(this.countedInCurrentPartition(ShortReadResponseProtection.this.singleResultCounter) == 0) {
               return null;
            } else if(DataResolver.this.command.metadata().clusteringColumns().isEmpty()) {
               return null;
            } else {
               this.lastFetched = this.countedInCurrentPartition(ShortReadResponseProtection.this.singleResultCounter) - this.lastCounted;
               this.lastCounted = this.countedInCurrentPartition(ShortReadResponseProtection.this.singleResultCounter);
               if(this.lastQueried > 0 && this.lastFetched < this.lastQueried) {
                  return null;
               } else {
                  this.lastQueried = Math.min(DataResolver.this.command.limits().count(), DataResolver.this.command.limits().perPartitionCount());
                  ColumnFamilyStore.metricsFor(DataResolver.this.command.metadata().id).shortReadProtectionRequests.mark();
                  if(ResponseResolver.logger.isTraceEnabled()) {
                     ResponseResolver.logger.trace("Requesting {} extra rows from {} for short read protection", Integer.valueOf(this.lastQueried), ShortReadResponseProtection.this.source);
                  }

                  Tracing.trace("Requesting {} extra rows from {} for short read protection", Integer.valueOf(this.lastQueried), ShortReadResponseProtection.this.source);
                  SinglePartitionReadCommand cmd = this.makeFetchAdditionalRowsReadCommand(this.lastQueried);
                  Flow<FlowableUnfilteredPartition> rows = ShortReadResponseProtection.this.executeReadCommand(cmd);
                  return DataLimits.countUnfilteredRows(rows.flatMap((fup) -> {
                     return fup.content();
                  }), ShortReadResponseProtection.this.singleResultCounter);
               }
            }
         }

         private SinglePartitionReadCommand makeFetchAdditionalRowsReadCommand(int toQuery) {
            ClusteringIndexFilter filter = DataResolver.this.command.clusteringIndexFilter(ShortReadResponseProtection.this.lastPartitionKey);
            if(null != this.lastClustering) {
               filter = filter.forPaging(DataResolver.this.command.metadata().comparator, this.lastClustering, false);
            }

            return SinglePartitionReadCommand.create(DataResolver.this.command.metadata(), DataResolver.this.command.nowInSec(), DataResolver.this.command.columnFilter(), DataResolver.this.command.rowFilter(), DataResolver.this.command.limits().forShortReadRetry(toQuery), ShortReadResponseProtection.this.lastPartitionKey, filter, DataResolver.this.command.indexMetadata(), DataResolver.this.command.getScheduler());
         }

         private int countedInCurrentPartition(DataLimits.Counter counter) {
            return DataResolver.this.command.limits().isGroupByLimit()?counter.rowCountedInCurrentPartition():counter.countedInCurrentPartition();
         }
      }
   }

   private class RepairMergeListener implements FlowablePartitions.MergeListener {
      private final InetAddress[] sources;
      private final ReadRepairFuture repairResults;

      private RepairMergeListener(InetAddress[] sources, ReadRepairFuture repairResults) {
         this.sources = sources;
         this.repairResults = repairResults;
      }

      public UnfilteredRowIterators.MergeListener getRowMergeListener(DecoratedKey partitionKey, FlowableUnfilteredPartition[] versions) {
         return new DataResolver.RepairMergeListener.MergeListener(partitionKey, this.columns(versions), this.isReversed(versions));
      }

      private RegularAndStaticColumns columns(FlowableUnfilteredPartition[] partitions) {
         Columns statics = Columns.NONE;
         Columns regulars = Columns.NONE;
         FlowableUnfilteredPartition[] var4 = partitions;
         int var5 = partitions.length;

         for(int var6 = 0; var6 < var5; ++var6) {
            FlowableUnfilteredPartition partition = var4[var6];
            if(partition != null) {
               RegularAndStaticColumns cols = partition.columns();
               statics = statics.mergeTo(cols.statics);
               regulars = regulars.mergeTo(cols.regulars);
            }
         }

         return new RegularAndStaticColumns(statics, regulars);
      }

      private boolean isReversed(FlowableUnfilteredPartition[] partitions) {
         FlowableUnfilteredPartition[] var2 = partitions;
         int var3 = partitions.length;

         for(int var4 = 0; var4 < var3; ++var4) {
            FlowableUnfilteredPartition partition = var2[var4];
            if(partition != null) {
               return partition.isReverseOrder();
            }
         }

         assert false : "Expected at least one iterator";

         return false;
      }

      private class MergeListener implements UnfilteredRowIterators.MergeListener {
         private final DecoratedKey partitionKey;
         private final RegularAndStaticColumns columns;
         private final boolean isReversed;
         private final PartitionUpdate[] repairs;
         private final Row.Builder[] currentRows;
         private final RowDiffListener diffListener;
         private DeletionTime partitionLevelDeletion;
         private DeletionTime mergedDeletionTime;
         private final DeletionTime[] sourceDeletionTime;
         private final ClusteringBound[] markerToRepair;

         private MergeListener(DecoratedKey partitionKey, RegularAndStaticColumns columns, boolean isReversed) {
            this.repairs = new PartitionUpdate[RepairMergeListener.this.sources.length];
            this.currentRows = new Row.Builder[RepairMergeListener.this.sources.length];
            this.sourceDeletionTime = new DeletionTime[RepairMergeListener.this.sources.length];
            this.markerToRepair = new ClusteringBound[RepairMergeListener.this.sources.length];
            this.partitionKey = partitionKey;
            this.columns = columns;
            this.isReversed = isReversed;
            this.diffListener = new RowDiffListener() {
               public void onPrimaryKeyLivenessInfo(int i, Clustering clustering, LivenessInfo merged, LivenessInfo original) {
                  if(merged != null && !merged.equals(original)) {
                     MergeListener.this.currentRow(i, clustering).addPrimaryKeyLivenessInfo(merged);
                  }

               }

               public void onDeletion(int i, Clustering clustering, Row.Deletion merged, Row.Deletion original) {
                  if(merged != null && !merged.equals(original)) {
                     MergeListener.this.currentRow(i, clustering).addRowDeletion(merged);
                  }

               }

               public void onComplexDeletion(int i, Clustering clustering, ColumnMetadata column, DeletionTime merged, DeletionTime original) {
                  if(merged != null && !merged.equals(original)) {
                     MergeListener.this.currentRow(i, clustering).addComplexDeletion(column, merged);
                  }

               }

               public void onCell(int i, Clustering clustering, Cell merged, Cell original) {
                  if(merged != null && !merged.equals(original) && this.isQueried(merged)) {
                     MergeListener.this.currentRow(i, clustering).addCell(merged);
                  }

               }

               private boolean isQueried(Cell cell) {
                  ColumnMetadata column = cell.column();
                  ColumnFilter filter = DataResolver.this.command.columnFilter();
                  return column.isComplex()?filter.fetchedCellIsQueried(column, cell.path()):filter.fetchedColumnIsQueried(column);
               }
            };
            if(DataResolver.this.ctx.readObserver != null) {
               DataResolver.this.ctx.readObserver.onPartition(partitionKey);
            }

         }

         private PartitionUpdate update(int i) {
            if(this.repairs[i] == null) {
               this.repairs[i] = new PartitionUpdate(DataResolver.this.command.metadata(), this.partitionKey, this.columns, 1);
            }

            return this.repairs[i];
         }

         private DeletionTime partitionLevelRepairDeletion(int i) {
            return this.repairs[i] == null?DeletionTime.LIVE:this.repairs[i].partitionLevelDeletion();
         }

         private Row.Builder currentRow(int i, Clustering clustering) {
            if(this.currentRows[i] == null) {
               this.currentRows[i] = Row.Builder.sorted();
               this.currentRows[i].newRow(clustering);
            }

            return this.currentRows[i];
         }

         public void onMergedPartitionLevelDeletion(DeletionTime mergedDeletion, DeletionTime[] versions) {
            this.partitionLevelDeletion = mergedDeletion;
            boolean isConsistent = true;

            for(int i = 0; i < versions.length; ++i) {
               if(mergedDeletion.supersedes(versions[i])) {
                  this.update(i).addPartitionDeletion(mergedDeletion);
                  isConsistent = false;
               }
            }

            if(DataResolver.this.ctx.readObserver != null && !mergedDeletion.isLive()) {
               DataResolver.this.ctx.readObserver.onPartitionDeletion(mergedDeletion, isConsistent);
            }

         }

         public void onMergedRows(Row merged, Row[] versions) {
            if(!merged.isEmpty()) {
               Rows.diff(this.diffListener, merged, versions);
               boolean isConsistent = true;

               for(int i = 0; i < this.currentRows.length; ++i) {
                  if(this.currentRows[i] != null) {
                     isConsistent = false;
                     this.update(i).add(this.currentRows[i].build());
                  }
               }

               Arrays.fill(this.currentRows, (Object)null);
               if(DataResolver.this.ctx.readObserver != null) {
                  DataResolver.this.ctx.readObserver.onRow(merged, isConsistent);
               }

            }
         }

         private DeletionTime currentDeletion() {
            return this.mergedDeletionTime == null?this.partitionLevelDeletion:this.mergedDeletionTime;
         }

         public void onMergedRangeTombstoneMarkers(RangeTombstoneMarker merged, RangeTombstoneMarker[] versions) {
            try {
               this.internalOnMergedRangeTombstoneMarkers(merged, versions);
            } catch (AssertionError var6) {
               TableMetadata table = DataResolver.this.command.metadata();
               String details = String.format("Error merging RTs on %s: merged=%s, versions=%s, sources={%s}, responses:%n %s", new Object[]{table, merged == null?"null":merged.toString(table), '[' + Joiner.on(", ").join(Iterables.transform(Arrays.asList(versions), (rt) -> {
                  return rt == null?"null":rt.toString(table);
               })) + ']', Arrays.toString(RepairMergeListener.this.sources), this.makeResponsesDebugString()});
               throw new AssertionError(details, var6);
            }
         }

         private String makeResponsesDebugString() {
            return Joiner.on(",\n").join(Iterables.transform(DataResolver.this.getMessages(), (m) -> {
               return m.from() + " => " + ((ReadResponse)m.payload()).toDebugString(DataResolver.this.command, this.partitionKey);
            }));
         }

         private void internalOnMergedRangeTombstoneMarkers(RangeTombstoneMarker merged, RangeTombstoneMarker[] versions) {
            boolean isConsistent = true;
            DeletionTime currentDeletion = this.currentDeletion();

            for(int i = 0; i < versions.length; ++i) {
               RangeTombstoneMarker marker = versions[i];
               if(marker != null) {
                  this.sourceDeletionTime[i] = marker.isOpen(this.isReversed)?marker.openDeletionTime(this.isReversed):null;
               }

               DeletionTime partitionRepairDeletion;
               if(merged == null) {
                  if(marker != null) {
                     assert !currentDeletion.isLive() : currentDeletion.toString();

                     partitionRepairDeletion = this.partitionLevelRepairDeletion(i);
                     if(this.markerToRepair[i] == null && currentDeletion.supersedes(partitionRepairDeletion)) {
                        if(!marker.isBoundary() && marker.isOpen(this.isReversed)) {
                           assert currentDeletion.equals(marker.openDeletionTime(this.isReversed)) : String.format("currentDeletion=%s, marker=%s", new Object[]{currentDeletion, marker.toString(DataResolver.this.command.metadata())});
                        } else {
                           assert marker.isClose(this.isReversed) && currentDeletion.equals(marker.closeDeletionTime(this.isReversed)) : String.format("currentDeletion=%s, marker=%s", new Object[]{currentDeletion, marker.toString(DataResolver.this.command.metadata())});
                        }

                        if(!marker.isOpen(this.isReversed) || !currentDeletion.equals(marker.openDeletionTime(this.isReversed))) {
                           this.markerToRepair[i] = marker.closeBound(this.isReversed).invert();
                           isConsistent = false;
                        }
                     } else if(marker.isOpen(this.isReversed) && currentDeletion.equals(marker.openDeletionTime(this.isReversed))) {
                        this.closeOpenMarker(i, marker.openBound(this.isReversed).invert());
                        isConsistent = false;
                     }
                  }
               } else {
                  if(merged.isClose(this.isReversed) && this.markerToRepair[i] != null) {
                     this.closeOpenMarker(i, merged.closeBound(this.isReversed));
                     isConsistent = false;
                  }

                  if(merged.isOpen(this.isReversed)) {
                     partitionRepairDeletion = merged.openDeletionTime(this.isReversed);
                     DeletionTime sourceDeletion = this.sourceDeletionTime[i];
                     if(!partitionRepairDeletion.equals(sourceDeletion)) {
                        this.markerToRepair[i] = merged.openBound(this.isReversed);
                        isConsistent = false;
                     }
                  }
               }
            }

            if(merged != null) {
               this.mergedDeletionTime = merged.isOpen(this.isReversed)?merged.openDeletionTime(this.isReversed):null;
            }

            if(DataResolver.this.ctx.readObserver != null) {
               DataResolver.this.ctx.readObserver.onRangeTombstoneMarker(merged, isConsistent);
            }

         }

         private void closeOpenMarker(int i, ClusteringBound close) {
            ClusteringBound open = this.markerToRepair[i];
            this.update(i).add(new RangeTombstone(Slice.make(this.isReversed?close:open, this.isReversed?open:close), this.currentDeletion()));
            this.markerToRepair[i] = null;
         }

         public void close() {
            for(int i = 0; i < this.repairs.length; ++i) {
               if(null != this.repairs[i]) {
                  this.sendRepairMutation(this.repairs[i], RepairMergeListener.this.sources[i], true);
               }
            }

         }

         private void sendRepairMutation(PartitionUpdate partition, InetAddress destination, boolean logOversizedMutation) {
            Mutation mutation = new Mutation(partition);
            Optional<MessagingVersion> optVersion = MessagingService.instance().getVersion(destination);
            if(!optVersion.isPresent()) {
               NoSpamLogger.log(ResponseResolver.logger, NoSpamLogger.Level.ERROR, 1L, TimeUnit.MINUTES, "Should send read-repair mutation to {} but its version is unknown: this should not happen and should be reported. This will only prevent the node to be read-repaired temporarily.", new Object[]{destination});
            } else {
               MessagingVersion messagingVersion = (MessagingVersion)optVersion.get();
               long mutationSize = ((Serializer)Mutation.serializers.get(messagingVersion.groupVersion(Verbs.Group.WRITES))).serializedSize(mutation);
               if(!CommitLog.isOversizedMutation(mutationSize)) {
                  Tracing.trace("Sending read-repair-mutation to {}", (Object)destination);
                  if(DataResolver.this.ctx.readObserver != null) {
                     DataResolver.this.ctx.readObserver.onRepair(destination, partition);
                  }

                  this.sendMutationInternal(mutation, destination);
               } else {
                  int opCount = partition.operationCount();
                  int batchSize;
                  if(opCount == 1) {
                     ResponseResolver.logger.error("Encountered an oversized ({}/{}) read repair mutation of one row for table {}, key {}, node {}. Increase max_mutation_size_in_kb on all nodes could mitigate the issue.", new Object[]{Long.valueOf(mutationSize), Integer.valueOf(DatabaseDescriptor.getMaxMutationSize()), DataResolver.this.command.metadata(), DataResolver.this.command.metadata().partitionKeyType.getString(this.partitionKey.getKey()), destination});
                     batchSize = DataResolver.this.ctx.requiredResponses();
                     Tracing.trace("Read repair failed after receiving all {} data and digest responses due to oversized readrepair mutation of one row", (Object)Integer.valueOf(batchSize));
                     throw new ReadFailureException(DataResolver.this.ctx.consistencyLevel, batchSize - 1, batchSize, true, ImmutableMap.of(destination, RequestFailureReason.UNKNOWN));
                  } else {
                     if(logOversizedMutation) {
                        ResponseResolver.logger.debug("Encountered an oversized ({}/{}) read repair mutation for table {}, key {}, node {}, will split the mutation by half", new Object[]{Long.valueOf(mutationSize), Integer.valueOf(DatabaseDescriptor.getMaxMutationSize()), DataResolver.this.command.metadata(), DataResolver.this.command.metadata().partitionKeyType.getString(this.partitionKey.getKey()), destination});
                     }

                     batchSize = (int)Math.ceil((double)opCount / 2.0D);
                     CloseableIterator<UnfilteredRowIterator> throttled = ThrottledUnfilteredIterator.throttle(partition.unfilteredIterator(), batchSize);
                     Throwable var12 = null;

                     try {
                        while(throttled.hasNext()) {
                           this.sendRepairMutation(PartitionUpdate.fromIterator((UnfilteredRowIterator)throttled.next(), DataResolver.this.command.columnFilter()), destination, false);
                        }
                     } catch (Throwable var21) {
                        var12 = var21;
                        throw var21;
                     } finally {
                        if(throttled != null) {
                           if(var12 != null) {
                              try {
                                 throttled.close();
                              } catch (Throwable var20) {
                                 var12.addSuppressed(var20);
                              }
                           } else {
                              throttled.close();
                           }
                        }

                     }

                  }
               }
            }
         }

         private void sendMutationInternal(Mutation mutation, InetAddress destination) {
            Request<Mutation, EmptyPayload> request = Verbs.WRITES.READ_REPAIR.newRequest(destination, mutation);
            MessagingService.instance().send(request, RepairMergeListener.this.repairResults.getRepairCallback());
            ColumnFamilyStore.metricsFor(DataResolver.this.command.metadata().id).readRepairRequests.mark();
         }
      }
   }
}
