package org.apache.cassandra.db.rows;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.SimpleBuilders;
import org.apache.cassandra.db.partitions.PartitionStatisticsCollector;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.NumberUtil;
import org.apache.cassandra.utils.Reducer;

public abstract class Rows {
   public static final Row EMPTY_STATIC_ROW;

   private Rows() {
   }

   public static Row.Builder copy(Row row, Row.Builder builder) {
      builder.newRow(row.clustering());
      builder.addPrimaryKeyLivenessInfo(row.primaryKeyLivenessInfo());
      builder.addRowDeletion(row.deletion());
      row.apply((cd) -> {
         if(cd.column().isSimple()) {
            builder.addCell((Cell)cd);
         } else {
            ComplexColumnData complexData = (ComplexColumnData)cd;
            builder.addComplexDeletion(complexData.column(), complexData.complexDeletion());
            Iterator var3 = complexData.iterator();

            while(var3.hasNext()) {
               Cell cell = (Cell)var3.next();
               builder.addCell(cell);
            }
         }

      }, false);
      return builder;
   }

   public static Row.SimpleBuilder simpleBuilder(TableMetadata metadata, Object... clusteringValues) {
      return new SimpleBuilders.RowBuilder(metadata, clusteringValues);
   }

   public static void collectStats(Row row, PartitionStatisticsCollector collector) {
      assert !row.isEmpty();

      collector.update(row.primaryKeyLivenessInfo());
      collector.update(row.deletion().time());
      row.apply((cd) -> {
         if(cd.column().isSimple()) {
            collector.update(cd);
            Cells.collectStats((Cell)cd, collector);
         } else {
            ComplexColumnData complexData = (ComplexColumnData)cd;
            collector.update(complexData.complexDeletion());
            if(complexData.hasCells()) {
               collector.update(cd);
               complexData.applyForwards((cell) -> {
                  Cells.collectStats(cell, collector);
               });
            }
         }

      }, false);
      collector.updateRowStats();
   }

   public static void diff(final RowDiffListener diffListener, Row merged, final Row... inputs) {
      final Clustering clustering = merged.clustering();
      LivenessInfo mergedInfo = merged.primaryKeyLivenessInfo().isEmpty()?null:merged.primaryKeyLivenessInfo();
      Row.Deletion mergedDeletion = merged.deletion().isLive()?null:merged.deletion();

      for(int i = 0; i < inputs.length; ++i) {
         Row input = inputs[i];
         LivenessInfo inputInfo = input != null && !input.primaryKeyLivenessInfo().isEmpty()?input.primaryKeyLivenessInfo():null;
         Row.Deletion inputDeletion = input != null && !input.deletion().isLive()?input.deletion():null;
         if(mergedInfo != null || inputInfo != null) {
            diffListener.onPrimaryKeyLivenessInfo(i, clustering, mergedInfo, inputInfo);
         }

         if(mergedDeletion != null || inputDeletion != null) {
            diffListener.onDeletion(i, clustering, mergedDeletion, inputDeletion);
         }
      }

      List<Iterator<ColumnData>> inputIterators = new ArrayList(1 + inputs.length);
      inputIterators.add(merged.iterator());
      Row[] var12 = inputs;
      int var14 = inputs.length;

      for(int var15 = 0; var15 < var14; ++var15) {
         Row row = var12[var15];
         inputIterators.add(row == null?Collections.emptyIterator():row.iterator());
      }

      MergeIterator iter = MergeIterator.get(inputIterators, ColumnData.comparator, new Reducer<ColumnData, Object>() {
         ColumnData mergedData;
         ColumnData[] inputDatas = new ColumnData[inputs.length];

         public void reduce(int idx, ColumnData current) {
            if(idx == 0) {
               this.mergedData = current;
            } else {
               this.inputDatas[idx - 1] = current;
            }

         }

         public Object getReduced() {
            for(int i = 0; i != this.inputDatas.length; ++i) {
               ColumnData input = this.inputDatas[i];
               if(this.mergedData != null || input != null) {
                  ColumnMetadata column = (this.mergedData != null?this.mergedData:input).column;
                  if(column.isSimple()) {
                     diffListener.onCell(i, clustering, (Cell)this.mergedData, (Cell)input);
                  } else {
                     ComplexColumnData mergedData = (ComplexColumnData)this.mergedData;
                     ComplexColumnData inputData = (ComplexColumnData)input;
                     Iterator var9;
                     Cell mergedCell;
                     if(mergedData == null) {
                        if(!inputData.complexDeletion().isLive()) {
                           diffListener.onComplexDeletion(i, clustering, column, (DeletionTime)null, inputData.complexDeletion());
                        }

                        var9 = inputData.iterator();

                        while(var9.hasNext()) {
                           mergedCell = (Cell)var9.next();
                           diffListener.onCell(i, clustering, (Cell)null, mergedCell);
                        }
                     } else if(inputData == null) {
                        if(!mergedData.complexDeletion().isLive()) {
                           diffListener.onComplexDeletion(i, clustering, column, mergedData.complexDeletion(), (DeletionTime)null);
                        }

                        var9 = mergedData.iterator();

                        while(var9.hasNext()) {
                           mergedCell = (Cell)var9.next();
                           diffListener.onCell(i, clustering, mergedCell, (Cell)null);
                        }
                     } else {
                        if(!mergedData.complexDeletion().isLive() || !inputData.complexDeletion().isLive()) {
                           diffListener.onComplexDeletion(i, clustering, column, mergedData.complexDeletion(), inputData.complexDeletion());
                        }

                        PeekingIterator<Cell> mergedCells = Iterators.peekingIterator(mergedData.iterator());
                        PeekingIterator inputCells = Iterators.peekingIterator(inputData.iterator());

                        while(mergedCells.hasNext() && inputCells.hasNext()) {
                           int cmp = column.cellPathComparator().compare(((Cell)mergedCells.peek()).path(), ((Cell)inputCells.peek()).path());
                           if(cmp == 0) {
                              diffListener.onCell(i, clustering, (Cell)mergedCells.next(), (Cell)inputCells.next());
                           } else if(cmp < 0) {
                              diffListener.onCell(i, clustering, (Cell)mergedCells.next(), (Cell)null);
                           } else {
                              diffListener.onCell(i, clustering, (Cell)null, (Cell)inputCells.next());
                           }
                        }

                        while(mergedCells.hasNext()) {
                           diffListener.onCell(i, clustering, (Cell)mergedCells.next(), (Cell)null);
                        }

                        while(inputCells.hasNext()) {
                           diffListener.onCell(i, clustering, (Cell)null, (Cell)inputCells.next());
                        }
                     }
                  }
               }
            }

            return null;
         }

         public void onKeyChange() {
            this.mergedData = null;
            Arrays.fill(this.inputDatas, (Object)null);
         }
      });

      while(iter.hasNext()) {
         iter.next();
      }

   }

   public static Row merge(Row row1, Row row2, int nowInSec) {
      Row.Builder builder = Row.Builder.sorted();
      merge(row1, row2, builder, nowInSec);
      return builder.build();
   }

   public static long merge(Row existing, Row update, Row.Builder builder, int nowInSec) {
      Clustering clustering = existing.clustering();
      builder.newRow(clustering);
      LivenessInfo existingInfo = existing.primaryKeyLivenessInfo();
      LivenessInfo updateInfo = update.primaryKeyLivenessInfo();
      LivenessInfo mergedInfo = existingInfo.supersedes(updateInfo)?existingInfo:updateInfo;
      long timeDelta = NumberUtil.consistentAbs(existingInfo.timestamp() - mergedInfo.timestamp());
      Row.Deletion rowDeletion = existing.deletion().supersedes(update.deletion())?existing.deletion():update.deletion();
      if(rowDeletion.deletes(mergedInfo)) {
         mergedInfo = LivenessInfo.EMPTY;
      } else if(rowDeletion.isShadowedBy(mergedInfo)) {
         rowDeletion = Row.Deletion.LIVE;
      }

      builder.addPrimaryKeyLivenessInfo(mergedInfo);
      builder.addRowDeletion(rowDeletion);
      DeletionTime deletion = rowDeletion.time();
      Iterator<ColumnData> a = existing.iterator();
      Iterator<ColumnData> b = update.iterator();
      ColumnData nexta = a.hasNext()?(ColumnData)a.next():null;
      ColumnData nextb = b.hasNext()?(ColumnData)b.next():null;

      while(nexta != null | nextb != null) {
         int comparison = nexta == null?1:(nextb == null?-1:nexta.column.compareTo(nextb.column));
         ColumnData cura = comparison <= 0?nexta:null;
         ColumnData curb = comparison >= 0?nextb:null;
         ColumnMetadata column = getColumnMetadata(cura, curb);
         if(column.isSimple()) {
            timeDelta = Math.min(timeDelta, Cells.reconcile((Cell)cura, (Cell)curb, deletion, builder, nowInSec));
         } else {
            ComplexColumnData existingData = (ComplexColumnData)cura;
            ComplexColumnData updateData = (ComplexColumnData)curb;
            DeletionTime existingDt = existingData == null?DeletionTime.LIVE:existingData.complexDeletion();
            DeletionTime updateDt = updateData == null?DeletionTime.LIVE:updateData.complexDeletion();
            DeletionTime maxDt = existingDt.supersedes(updateDt)?existingDt:updateDt;
            if(maxDt.supersedes(deletion)) {
               builder.addComplexDeletion(column, maxDt);
            } else {
               maxDt = deletion;
            }

            Iterator<Cell> existingCells = existingData == null?null:existingData.iterator();
            Iterator<Cell> updateCells = updateData == null?null:updateData.iterator();
            timeDelta = Math.min(timeDelta, Cells.reconcileComplex(column, existingCells, updateCells, maxDt, builder, nowInSec));
         }

         if(cura != null) {
            nexta = a.hasNext()?(ColumnData)a.next():null;
         }

         if(curb != null) {
            nextb = b.hasNext()?(ColumnData)b.next():null;
         }
      }

      return timeDelta;
   }

   public static Row removeShadowedCells(Row existing, Row update, DeletionTime rangeDeletion, int nowInSec) {
      Row.Builder builder = Row.Builder.sorted();
      Clustering clustering = existing.clustering();
      builder.newRow(clustering);
      DeletionTime deletion = update.deletion().time();
      if(rangeDeletion.supersedes(deletion)) {
         deletion = rangeDeletion;
      }

      LivenessInfo existingInfo = existing.primaryKeyLivenessInfo();
      if(!deletion.deletes(existingInfo)) {
         builder.addPrimaryKeyLivenessInfo(existingInfo);
      }

      Row.Deletion rowDeletion = existing.deletion();
      if(!deletion.supersedes(rowDeletion.time())) {
         builder.addRowDeletion(rowDeletion);
      }

      Iterator<ColumnData> a = existing.iterator();
      Iterator<ColumnData> b = update.iterator();
      ColumnData nexta = a.hasNext()?(ColumnData)a.next():null;
      ColumnData nextb = b.hasNext()?(ColumnData)b.next():null;

      while(nexta != null) {
         int comparison = nextb == null?-1:nexta.column.compareTo(nextb.column);
         if(comparison <= 0) {
            ColumnMetadata column = nexta.column;
            ColumnData curb = comparison == 0?nextb:null;
            if(column.isSimple()) {
               Cells.addNonShadowed((Cell)nexta, (Cell)curb, deletion, builder, nowInSec);
            } else {
               ComplexColumnData existingData = (ComplexColumnData)nexta;
               ComplexColumnData updateData = (ComplexColumnData)curb;
               DeletionTime existingDt = existingData.complexDeletion();
               DeletionTime updateDt = updateData == null?DeletionTime.LIVE:updateData.complexDeletion();
               DeletionTime maxDt = updateDt.supersedes(deletion)?updateDt:deletion;
               if(existingDt.supersedes(maxDt)) {
                  builder.addComplexDeletion(column, existingDt);
                  maxDt = existingDt;
               }

               Iterator<Cell> existingCells = existingData.iterator();
               Iterator<Cell> updateCells = updateData == null?null:updateData.iterator();
               Cells.addNonShadowedComplex(column, existingCells, updateCells, maxDt, builder, nowInSec);
            }

            nexta = a.hasNext()?(ColumnData)a.next():null;
            if(curb != null) {
               nextb = b.hasNext()?(ColumnData)b.next():null;
            }
         } else {
            nextb = b.hasNext()?(ColumnData)b.next():null;
         }
      }

      Row row = builder.build();
      return row != null && !row.isEmpty()?row:null;
   }

   private static ColumnMetadata getColumnMetadata(ColumnData cura, ColumnData curb) {
      return cura == null?curb.column:(curb == null?cura.column:(AbstractTypeVersionComparator.INSTANCE.compare(cura.column.type, curb.column.type) >= 0?cura.column:curb.column));
   }

   static {
      EMPTY_STATIC_ROW = ArrayBackedRow.emptyRow(Clustering.STATIC_CLUSTERING);
   }
}
