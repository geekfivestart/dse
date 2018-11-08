package org.apache.cassandra.db.view;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import io.reactivex.functions.Function;
import java.nio.ByteBuffer;
import java.util.Iterator;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.flow.Flow;

public class ViewUpdateGenerator {
   private final View view;
   private final int nowInSec;
   private final TableMetadata baseMetadata;
   private final DecoratedKey baseDecoratedKey;
   private final ByteBuffer[] basePartitionKey;
   private final TableMetadata viewMetadata;
   private final boolean baseEnforceStrictLiveness;
   private final ByteBuffer[] currentViewEntryPartitionKey;
   private final Row.Builder currentViewEntryBuilder;

   public ViewUpdateGenerator(View view, DecoratedKey basePartitionKey, int nowInSec) {
      this.view = view;
      this.nowInSec = nowInSec;
      this.baseMetadata = view.getDefinition().baseTableMetadata();
      this.baseEnforceStrictLiveness = this.baseMetadata.enforceStrictLiveness();
      this.baseDecoratedKey = basePartitionKey;
      this.basePartitionKey = extractKeyComponents(basePartitionKey, this.baseMetadata.partitionKeyType);
      this.viewMetadata = Schema.instance.getTableMetadata(view.getDefinition().metadata.id);
      this.currentViewEntryPartitionKey = new ByteBuffer[this.viewMetadata.partitionKeyColumns().size()];
      this.currentViewEntryBuilder = Row.Builder.sorted();
   }

   private static ByteBuffer[] extractKeyComponents(DecoratedKey partitionKey, AbstractType<?> type) {
      return type instanceof CompositeType?((CompositeType)type).split(partitionKey.getKey()):new ByteBuffer[]{partitionKey.getKey()};
   }

   protected Flow<PartitionUpdate> createViewUpdates(Row existingBaseRow, Row mergedBaseRow) {
      Flow flowableViewUpdates;
      switch(null.$SwitchMap$org$apache$cassandra$db$view$ViewUpdateGenerator$UpdateAction[this.updateAction(existingBaseRow, mergedBaseRow).ordinal()]) {
      case 1:
         flowableViewUpdates = Flow.empty();
         break;
      case 2:
         flowableViewUpdates = this.createEntry(mergedBaseRow);
         break;
      case 3:
         flowableViewUpdates = this.deleteOldEntry(existingBaseRow, mergedBaseRow);
         break;
      case 4:
         flowableViewUpdates = this.updateEntry(existingBaseRow, mergedBaseRow);
         break;
      case 5:
         flowableViewUpdates = Flow.concat(new Flow[]{this.createEntry(mergedBaseRow), this.deleteOldEntry(existingBaseRow, mergedBaseRow)});
         break;
      default:
         throw new AssertionError();
      }

      return flowableViewUpdates;
   }

   private ViewUpdateGenerator.UpdateAction updateAction(Row existingBaseRow, Row mergedBaseRow) {
      assert !mergedBaseRow.isEmpty();

      if(this.baseMetadata.isCompactTable()) {
         Clustering clustering = mergedBaseRow.clustering();

         for(int i = 0; i < clustering.size(); ++i) {
            if(clustering.get(i) == null) {
               return ViewUpdateGenerator.UpdateAction.NONE;
            }
         }
      }

      assert this.view.baseNonPKColumnsInViewPK.size() <= 1 : "We currently only support one base non-PK column in the view PK";

      if(!this.view.baseNonPKColumnsInViewPK.isEmpty()) {
         ColumnMetadata baseColumn = (ColumnMetadata)this.view.baseNonPKColumnsInViewPK.get(0);

         assert !baseColumn.isComplex() : "A complex column couldn't be part of the view PK";

         Cell before = existingBaseRow == null?null:existingBaseRow.getCell(baseColumn);
         Cell after = mergedBaseRow.getCell(baseColumn);
         return before == after?(this.isLive(before)?ViewUpdateGenerator.UpdateAction.UPDATE_EXISTING:ViewUpdateGenerator.UpdateAction.NONE):(!this.isLive(before)?(this.isLive(after)?ViewUpdateGenerator.UpdateAction.NEW_ENTRY:ViewUpdateGenerator.UpdateAction.NONE):(!this.isLive(after)?ViewUpdateGenerator.UpdateAction.DELETE_OLD:(baseColumn.cellValueType().compare(before.value(), after.value()) == 0?ViewUpdateGenerator.UpdateAction.UPDATE_EXISTING:ViewUpdateGenerator.UpdateAction.SWITCH_ENTRY)));
      } else {
         boolean existingHasLiveData = existingBaseRow != null && existingBaseRow.hasLiveData(this.nowInSec, this.baseEnforceStrictLiveness);
         boolean mergedHasLiveData = mergedBaseRow.hasLiveData(this.nowInSec, this.baseEnforceStrictLiveness);
         return existingHasLiveData?(mergedHasLiveData?ViewUpdateGenerator.UpdateAction.UPDATE_EXISTING:ViewUpdateGenerator.UpdateAction.DELETE_OLD):(mergedHasLiveData?ViewUpdateGenerator.UpdateAction.NEW_ENTRY:ViewUpdateGenerator.UpdateAction.NONE);
      }
   }

   private Flow<Boolean> matchesViewFilter(Row baseRow) {
      return this.view.matchesViewFilter(this.baseDecoratedKey, baseRow, this.nowInSec);
   }

   private boolean isLive(Cell cell) {
      return cell != null && cell.isLive(this.nowInSec);
   }

   private Flow<PartitionUpdate> createEntry(Row baseRow) {
      return this.matchesViewFilter(baseRow).skippingMap((f) -> {
         if(!f.booleanValue()) {
            return null;
         } else {
            this.startNewUpdate(baseRow);
            this.currentViewEntryBuilder.addPrimaryKeyLivenessInfo(this.computeLivenessInfoForEntry(baseRow));
            this.currentViewEntryBuilder.addRowDeletion(baseRow.deletion());
            Iterator var3 = baseRow.iterator();

            while(var3.hasNext()) {
               ColumnData data = (ColumnData)var3.next();
               ColumnMetadata viewColumn = this.view.getViewColumn(data.column());
               if(viewColumn != null && !viewColumn.isPrimaryKeyColumn()) {
                  this.addColumnData(viewColumn, data);
               }
            }

            return this.submitUpdate();
         }
      });
   }

   private Flow<PartitionUpdate> updateEntry(Row existingBaseRow, Row mergedBaseRow) {
      return this.matchesViewFilter(existingBaseRow).flatMap((f) -> {
         return !f.booleanValue()?this.createEntry(mergedBaseRow):this.matchesViewFilter(mergedBaseRow).skippingMap((s) -> {
            return !s.booleanValue()?this.deleteOldEntryInternal(existingBaseRow, mergedBaseRow):this.updateEntryInternal(existingBaseRow, mergedBaseRow);
         });
      });
   }

   private PartitionUpdate updateEntryInternal(Row existingBaseRow, Row mergedBaseRow) {
      this.startNewUpdate(mergedBaseRow);
      this.currentViewEntryBuilder.addPrimaryKeyLivenessInfo(this.computeLivenessInfoForEntry(mergedBaseRow));
      this.currentViewEntryBuilder.addRowDeletion(mergedBaseRow.deletion());
      this.addDifferentCells(existingBaseRow, mergedBaseRow);
      return this.submitUpdate();
   }

   private void addDifferentCells(Row existingBaseRow, Row mergedBaseRow) {
      PeekingIterator<ColumnData> existingIter = Iterators.peekingIterator(existingBaseRow.iterator());
      Iterator var4 = mergedBaseRow.iterator();

      while(true) {
         while(true) {
            ColumnData mergedData;
            ColumnMetadata baseColumn;
            ColumnMetadata viewColumn;
            do {
               do {
                  if(!var4.hasNext()) {
                     return;
                  }

                  mergedData = (ColumnData)var4.next();
                  baseColumn = mergedData.column();
                  viewColumn = this.view.getViewColumn(baseColumn);
               } while(viewColumn == null);
            } while(viewColumn.isPrimaryKeyColumn());

            ColumnData existingData = null;

            while(existingIter.hasNext()) {
               int cmp = baseColumn.compareTo(((ColumnData)existingIter.peek()).column());
               if(cmp < 0) {
                  break;
               }

               ColumnData next = (ColumnData)existingIter.next();
               if(cmp == 0) {
                  existingData = next;
                  break;
               }
            }

            if(existingData == null) {
               this.addColumnData(viewColumn, mergedData);
            } else if(mergedData != existingData) {
               if(!baseColumn.isComplex()) {
                  this.addCell(viewColumn, (Cell)mergedData);
               } else {
                  ComplexColumnData mergedComplexData = (ComplexColumnData)mergedData;
                  ComplexColumnData existingComplexData = (ComplexColumnData)existingData;
                  if(mergedComplexData.complexDeletion().supersedes(existingComplexData.complexDeletion())) {
                     this.currentViewEntryBuilder.addComplexDeletion(viewColumn, mergedComplexData.complexDeletion());
                  }

                  PeekingIterator<Cell> existingCells = Iterators.peekingIterator(existingComplexData.iterator());
                  Iterator var12 = mergedComplexData.iterator();

                  while(var12.hasNext()) {
                     Cell mergedCell = (Cell)var12.next();
                     Cell existingCell = null;

                     while(existingCells.hasNext()) {
                        int cmp = baseColumn.cellPathComparator().compare(mergedCell.path(), ((Cell)existingCells.peek()).path());
                        if(cmp > 0) {
                           break;
                        }

                        Cell next = (Cell)existingCells.next();
                        if(cmp == 0) {
                           existingCell = next;
                           break;
                        }
                     }

                     if(mergedCell != existingCell) {
                        this.addCell(viewColumn, mergedCell);
                     }
                  }
               }
            }
         }
      }
   }

   private Flow<PartitionUpdate> deleteOldEntry(Row existingBaseRow, Row mergedBaseRow) {
      return this.matchesViewFilter(existingBaseRow).skippingMap((f) -> {
         return !f.booleanValue()?null:this.deleteOldEntryInternal(existingBaseRow, mergedBaseRow);
      });
   }

   private PartitionUpdate deleteOldEntryInternal(Row existingBaseRow, Row mergedBaseRow) {
      this.startNewUpdate(existingBaseRow);
      long timestamp = this.computeTimestampForEntryDeletion(existingBaseRow, mergedBaseRow);
      long rowDeletion = mergedBaseRow.deletion().time().markedForDeleteAt();

      assert timestamp >= rowDeletion;

      if(timestamp > rowDeletion) {
         LivenessInfo info = LivenessInfo.withExpirationTime(timestamp, 2147483647, this.nowInSec);
         this.currentViewEntryBuilder.addPrimaryKeyLivenessInfo(info);
      }

      this.currentViewEntryBuilder.addRowDeletion(mergedBaseRow.deletion());
      this.addDifferentCells(existingBaseRow, mergedBaseRow);
      return this.submitUpdate();
   }

   private void startNewUpdate(Row baseRow) {
      ByteBuffer[] clusteringValues = new ByteBuffer[this.viewMetadata.clusteringColumns().size()];
      Iterator var3 = this.viewMetadata.primaryKeyColumns().iterator();

      while(var3.hasNext()) {
         ColumnMetadata viewColumn = (ColumnMetadata)var3.next();
         ColumnMetadata baseColumn = this.view.getBaseColumn(viewColumn);
         ByteBuffer value = this.getValueForPK(baseColumn, baseRow);
         if(viewColumn.isPartitionKey()) {
            this.currentViewEntryPartitionKey[viewColumn.position()] = value;
         } else {
            clusteringValues[viewColumn.position()] = value;
         }
      }

      this.currentViewEntryBuilder.newRow(Clustering.make(clusteringValues));
   }

   private LivenessInfo computeLivenessInfoForEntry(Row baseRow) {
      assert this.view.baseNonPKColumnsInViewPK.size() <= 1;

      LivenessInfo baseLiveness = baseRow.primaryKeyLivenessInfo();
      if(this.view.baseNonPKColumnsInViewPK.isEmpty()) {
         if(this.view.getDefinition().includeAllColumns) {
            return baseLiveness;
         } else {
            long timestamp = baseLiveness.timestamp();
            boolean hasNonExpiringLiveCell = false;
            Cell biggestExpirationCell = null;
            Iterator var7 = baseRow.cells().iterator();

            while(var7.hasNext()) {
               Cell cell = (Cell)var7.next();
               if(this.view.getViewColumn(cell.column()) == null && this.isLive(cell)) {
                  timestamp = Math.max(timestamp, cell.maxTimestamp());
                  if(!cell.isExpiring()) {
                     hasNonExpiringLiveCell = true;
                  } else if(biggestExpirationCell == null) {
                     biggestExpirationCell = cell;
                  } else if(cell.localDeletionTime() > biggestExpirationCell.localDeletionTime()) {
                     biggestExpirationCell = cell;
                  }
               }
            }

            if(baseLiveness.isLive(this.nowInSec) && !baseLiveness.isExpiring()) {
               return LivenessInfo.create(timestamp, this.nowInSec);
            } else if(hasNonExpiringLiveCell) {
               return LivenessInfo.create(timestamp, this.nowInSec);
            } else if(biggestExpirationCell == null) {
               return baseLiveness;
            } else if(biggestExpirationCell.localDeletionTime() <= baseLiveness.localExpirationTime() && baseLiveness.isLive(this.nowInSec)) {
               return baseLiveness;
            } else {
               return LivenessInfo.withExpirationTime(timestamp, biggestExpirationCell.ttl(), biggestExpirationCell.localDeletionTime());
            }
         }
      } else {
         Cell cell = baseRow.getCell((ColumnMetadata)this.view.baseNonPKColumnsInViewPK.get(0));

         assert this.isLive(cell) : "We shouldn't have got there if the base row had no associated entry";

         return LivenessInfo.withExpirationTime(cell.timestamp(), cell.ttl(), cell.localDeletionTime());
      }
   }

   private long computeTimestampForEntryDeletion(Row existingBaseRow, Row mergedBaseRow) {
      DeletionTime deletion = mergedBaseRow.deletion().time();
      if(this.view.hasSamePrimaryKeyColumnsAsBaseTable()) {
         long timestamp = Math.max(deletion.markedForDeleteAt(), existingBaseRow.primaryKeyLivenessInfo().timestamp());
         if(this.view.getDefinition().includeAllColumns) {
            return timestamp;
         } else {
            Iterator var6 = existingBaseRow.cells().iterator();

            while(var6.hasNext()) {
               Cell cell = (Cell)var6.next();
               if(this.view.getViewColumn(cell.column()) == null) {
                  timestamp = Math.max(timestamp, cell.maxTimestamp());
               }
            }

            return timestamp;
         }
      } else {
         Cell before = existingBaseRow.getCell((ColumnMetadata)this.view.baseNonPKColumnsInViewPK.get(0));

         assert this.isLive(before) : "We shouldn't have got there if the base row had no associated entry";

         return deletion.deletes(before)?deletion.markedForDeleteAt():before.timestamp();
      }
   }

   private void addColumnData(ColumnMetadata viewColumn, ColumnData baseTableData) {
      assert viewColumn.isComplex() == baseTableData.column().isComplex();

      if(!viewColumn.isComplex()) {
         this.addCell(viewColumn, (Cell)baseTableData);
      } else {
         ComplexColumnData complexData = (ComplexColumnData)baseTableData;
         this.currentViewEntryBuilder.addComplexDeletion(viewColumn, complexData.complexDeletion());
         Iterator var4 = complexData.iterator();

         while(var4.hasNext()) {
            Cell cell = (Cell)var4.next();
            this.addCell(viewColumn, cell);
         }

      }
   }

   private void addCell(ColumnMetadata viewColumn, Cell baseTableCell) {
      assert !viewColumn.isPrimaryKeyColumn();

      this.currentViewEntryBuilder.addCell(baseTableCell.withUpdatedColumn(viewColumn));
   }

   private PartitionUpdate submitUpdate() {
      Row row = this.currentViewEntryBuilder.build();
      if(row.isEmpty()) {
         return null;
      } else {
         DecoratedKey partitionKey = this.makeCurrentPartitionKey();
         PartitionUpdate update = new PartitionUpdate(this.viewMetadata, partitionKey, this.viewMetadata.regularAndStaticColumns(), 4);
         update.add(row);
         return update;
      }
   }

   private DecoratedKey makeCurrentPartitionKey() {
      ByteBuffer rawKey = this.viewMetadata.partitionKeyColumns().size() == 1?this.currentViewEntryPartitionKey[0]:CompositeType.build(this.currentViewEntryPartitionKey);
      return this.viewMetadata.partitioner.decorateKey(rawKey);
   }

   private ByteBuffer getValueForPK(ColumnMetadata column, Row row) {
      switch(null.$SwitchMap$org$apache$cassandra$schema$ColumnMetadata$Kind[column.kind.ordinal()]) {
      case 1:
         return this.basePartitionKey[column.position()];
      case 2:
         return row.clustering().get(column.position());
      default:
         return row.getCell(column).value();
      }
   }

   private static enum UpdateAction {
      NONE,
      NEW_ENTRY,
      DELETE_OLD,
      UPDATE_EXISTING,
      SWITCH_ENTRY;

      private UpdateAction() {
      }
   }
}
