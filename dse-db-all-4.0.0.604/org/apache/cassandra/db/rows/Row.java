package org.apache.cassandra.db.rows;

import com.google.common.base.Predicate;
import com.google.common.hash.Hasher;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.HashingUtils;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.Reducer;
import org.apache.cassandra.utils.btree.BTree;

public interface Row extends Unfiltered, Collection<ColumnData> {
   Clustering clustering();

   Collection<ColumnMetadata> columns();

   Row.Deletion deletion();

   LivenessInfo primaryKeyLivenessInfo();

   boolean isStatic();

   boolean isEmpty();

   boolean hasLiveData(int var1, boolean var2);

   Cell getCell(ColumnMetadata var1);

   Cell getCell(ColumnMetadata var1, CellPath var2);

   ComplexColumnData getComplexColumnData(ColumnMetadata var1);

   Iterable<Cell> cells();

   Iterable<Cell> cellsInLegacyOrder(TableMetadata var1, boolean var2);

   boolean hasComplexDeletion();

   boolean hasComplex();

   boolean hasDeletion(int var1);

   Row filter(ColumnFilter var1, TableMetadata var2);

   Row filter(ColumnFilter var1, DeletionTime var2, boolean var3, TableMetadata var4);

   Row purge(DeletionPurger var1, int var2, boolean var3);

   Row withOnlyQueriedData(ColumnFilter var1);

   Row markCounterLocalToBeCleared();

   Row updateAllTimestamp(long var1);

   Row withRowDeletion(DeletionTime var1);

   int dataSize();

   long unsharedHeapSizeExcludingData();

   String toString(TableMetadata var1, boolean var2);

   void apply(Consumer<ColumnData> var1, boolean var2);

   void apply(Consumer<ColumnData> var1, Predicate<ColumnData> var2, boolean var3);

   <R> R reduce(R var1, BTree.ReduceFunction<R, ColumnData> var2);

   <R> R reduceCells(R var1, BTree.ReduceFunction<R, Cell> var2);

   public static class Merger {
      private final Row[] rows;
      private final List<Iterator<ColumnData>> columnDataIterators;
      private Clustering clustering;
      private int rowsToMerge;
      private int lastRowSet = -1;
      private final ColumnData[] dataBuffer;
      private final Row.Merger.ColumnDataReducer columnDataReducer;

      public Merger(int numRows, int nowInSec, int numColumns, boolean hasComplex) {
         this.rows = new Row[numRows];
         this.columnDataIterators = new ArrayList(numRows);
         this.columnDataReducer = new Row.Merger.ColumnDataReducer(numRows, nowInSec, hasComplex);
         this.dataBuffer = new ColumnData[numColumns];
      }

      public void clear() {
         Arrays.fill(this.rows, (Object)null);
         this.columnDataIterators.clear();
         this.rowsToMerge = 0;
         this.lastRowSet = -1;
      }

      public void add(int i, Row row) {
         this.clustering = row.clustering();
         this.rows[i] = row;
         ++this.rowsToMerge;
         this.lastRowSet = i;
      }

      public Row merge(DeletionTime activeDeletion) {
         if(this.rowsToMerge == 1 && activeDeletion.isLive()) {
            Row row = this.rows[this.lastRowSet];

            assert row != null;

            return row;
         } else {
            LivenessInfo rowInfo = LivenessInfo.EMPTY;
            Row.Deletion rowDeletion = Row.Deletion.LIVE;
            Row[] var4 = this.rows;
            int numMerged = var4.length;

            int var6;
            Row row;
            for(var6 = 0; var6 < numMerged; ++var6) {
               row = var4[var6];
               if(row != null) {
                  if(row.primaryKeyLivenessInfo().supersedes(rowInfo)) {
                     rowInfo = row.primaryKeyLivenessInfo();
                  }

                  if(row.deletion().supersedes(rowDeletion)) {
                     rowDeletion = row.deletion();
                  }
               }
            }

            if(rowDeletion.isShadowedBy(rowInfo)) {
               rowDeletion = Row.Deletion.LIVE;
            }

            if(rowDeletion.supersedes(activeDeletion)) {
               activeDeletion = rowDeletion.time();
            } else {
               rowDeletion = Row.Deletion.LIVE;
            }

            if(activeDeletion.deletes(rowInfo)) {
               rowInfo = LivenessInfo.EMPTY;
            }

            var4 = this.rows;
            numMerged = var4.length;

            for(var6 = 0; var6 < numMerged; ++var6) {
               row = var4[var6];
               if(row != null) {
                  this.columnDataIterators.add(row.iterator());
               }
            }

            this.columnDataReducer.setActiveDeletion(activeDeletion);
            Iterator<ColumnData> merged = MergeIterator.get(this.columnDataIterators, ColumnData.comparator, this.columnDataReducer);
            numMerged = 0;

            while(merged.hasNext()) {
               ColumnData data = (ColumnData)merged.next();
               if(data != null) {
                  this.dataBuffer[numMerged++] = data;
               }
            }

            return rowInfo.isEmpty() && rowDeletion.isLive() && numMerged == 0?null:ArrayBackedRow.create(this.clustering, rowInfo, rowDeletion, (ColumnData[])Arrays.copyOf(this.dataBuffer, numMerged), numMerged);
         }
      }

      public Clustering mergedClustering() {
         return this.clustering;
      }

      public Row[] mergedRows() {
         return this.rows;
      }

      private static class CellReducer extends Reducer<Cell, Cell> {
         private final int nowInSec;
         private DeletionTime activeDeletion;
         private Cell merged;

         public CellReducer(int nowInSec) {
            this.nowInSec = nowInSec;
         }

         public void setActiveDeletion(DeletionTime activeDeletion) {
            this.activeDeletion = activeDeletion;
            this.onKeyChange();
         }

         public void reduce(int idx, Cell cell) {
            if(!this.activeDeletion.deletes(cell)) {
               this.merged = this.merged == null?cell:Cells.reconcile(this.merged, cell, this.nowInSec);
            }

         }

         public Cell getReduced() {
            return this.merged;
         }

         public void onKeyChange() {
            this.merged = null;
         }
      }

      private static class ColumnDataReducer extends Reducer<ColumnData, ColumnData> {
         private final int nowInSec;
         private ColumnMetadata column;
         private final List<ColumnData> versions;
         private DeletionTime activeDeletion;
         private final ComplexColumnData.Builder complexBuilder;
         private final List<Iterator<Cell>> complexCells;
         private final Row.Merger.CellReducer cellReducer;

         public ColumnDataReducer(int size, int nowInSec, boolean hasComplex) {
            this.nowInSec = nowInSec;
            this.versions = new ArrayList(size);
            this.complexBuilder = hasComplex?ComplexColumnData.builder():null;
            this.complexCells = hasComplex?new ArrayList(size):null;
            this.cellReducer = new Row.Merger.CellReducer(nowInSec);
         }

         public void setActiveDeletion(DeletionTime activeDeletion) {
            this.activeDeletion = activeDeletion;
         }

         public void reduce(int idx, ColumnData data) {
            if(this.useColumnMetadata(data.column())) {
               this.column = data.column();
            }

            this.versions.add(data);
         }

         private boolean useColumnMetadata(ColumnMetadata dataColumn) {
            return this.column == null?true:AbstractTypeVersionComparator.INSTANCE.compare(this.column.type, dataColumn.type) < 0;
         }

         public ColumnData getReduced() {
            Iterator var2;
            ColumnData data;
            if(this.column.isSimple()) {
               Cell merged = null;
               var2 = this.versions.iterator();

               while(var2.hasNext()) {
                  data = (ColumnData)var2.next();
                  Cell cell = (Cell)data;
                  if(!this.activeDeletion.deletes(cell)) {
                     merged = merged == null?cell:Cells.reconcile(merged, cell, this.nowInSec);
                  }
               }

               return merged;
            } else {
               this.complexBuilder.newColumn(this.column);
               this.complexCells.clear();
               DeletionTime complexDeletion = DeletionTime.LIVE;

               ComplexColumnData cd;
               for(var2 = this.versions.iterator(); var2.hasNext(); this.complexCells.add(cd.iterator())) {
                  data = (ColumnData)var2.next();
                  cd = (ComplexColumnData)data;
                  if(cd.complexDeletion().supersedes(complexDeletion)) {
                     complexDeletion = cd.complexDeletion();
                  }
               }

               if(complexDeletion.supersedes(this.activeDeletion)) {
                  this.cellReducer.setActiveDeletion(complexDeletion);
                  this.complexBuilder.addComplexDeletion(complexDeletion);
               } else {
                  this.cellReducer.setActiveDeletion(this.activeDeletion);
               }

               MergeIterator cells = MergeIterator.get(this.complexCells, Cell.comparator, this.cellReducer);

               while(cells.hasNext()) {
                  Cell merged = (Cell)cells.next();
                  if(merged != null) {
                     this.complexBuilder.addCell(merged);
                  }
               }

               return this.complexBuilder.build();
            }
         }

         public void onKeyChange() {
            this.column = null;
            this.versions.clear();
         }
      }
   }

   public interface SimpleBuilder {
      Row.SimpleBuilder timestamp(long var1);

      Row.SimpleBuilder ttl(int var1);

      Row.SimpleBuilder add(String var1, Object var2);

      Row.SimpleBuilder appendAll(String var1, Object var2);

      Row.SimpleBuilder delete();

      Row.SimpleBuilder delete(String var1);

      Row.SimpleBuilder noPrimaryKeyLivenessInfo();

      Row build();
   }

   public interface Builder {
      static default Row.Builder sorted() {
         return ArrayBackedRow.sortedBuilder();
      }

      static default Row.Builder unsorted(int nowInSec) {
         return ArrayBackedRow.unsortedBuilder(nowInSec);
      }

      Row.Builder copy();

      boolean isSorted();

      void newRow(Clustering var1);

      Clustering clustering();

      void addPrimaryKeyLivenessInfo(LivenessInfo var1);

      void addRowDeletion(Row.Deletion var1);

      void addCell(Cell var1);

      void addComplexDeletion(ColumnMetadata var1, DeletionTime var2);

      Row build();

      void reset();
   }

   public static class Deletion {
      public static final Row.Deletion LIVE;
      private final DeletionTime time;
      private final boolean isShadowable;

      public Deletion(DeletionTime time, boolean isShadowable) {
         assert !time.isLive() || !isShadowable;

         this.time = time;
         this.isShadowable = isShadowable;
      }

      public static Row.Deletion regular(DeletionTime time) {
         return time.isLive()?LIVE:new Row.Deletion(time, false);
      }

      /** @deprecated */
      @Deprecated
      public static Row.Deletion shadowable(DeletionTime time) {
         return new Row.Deletion(time, true);
      }

      public DeletionTime time() {
         return this.time;
      }

      public boolean isShadowable() {
         return this.isShadowable;
      }

      public boolean isLive() {
         return this.time().isLive();
      }

      public boolean supersedes(DeletionTime that) {
         return this.time.supersedes(that);
      }

      public boolean supersedes(Row.Deletion that) {
         return this.time.supersedes(that.time);
      }

      public boolean isShadowedBy(LivenessInfo primaryKeyLivenessInfo) {
         return this.isShadowable && primaryKeyLivenessInfo.timestamp() > this.time.markedForDeleteAt();
      }

      public boolean deletes(LivenessInfo info) {
         return this.time.deletes(info);
      }

      public boolean deletes(Cell cell) {
         return this.time.deletes(cell);
      }

      public void digest(Hasher hasher) {
         this.time.digest(hasher);
         HashingUtils.updateWithBoolean(hasher, this.isShadowable);
      }

      public int dataSize() {
         return this.time.dataSize() + 1;
      }

      public boolean equals(Object o) {
         if(!(o instanceof Row.Deletion)) {
            return false;
         } else {
            Row.Deletion that = (Row.Deletion)o;
            return this.time.equals(that.time) && this.isShadowable == that.isShadowable;
         }
      }

      public final int hashCode() {
         return Objects.hash(new Object[]{this.time, Boolean.valueOf(this.isShadowable)});
      }

      public String toString() {
         return String.format("%s%s", new Object[]{this.time, this.isShadowable?"(shadowable)":""});
      }

      static {
         LIVE = new Row.Deletion(DeletionTime.LIVE, false);
      }
   }
}
