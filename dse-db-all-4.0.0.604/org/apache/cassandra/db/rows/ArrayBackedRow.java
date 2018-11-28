package org.apache.cassandra.db.rows;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.function.Consumer;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIndexedListIterator;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.btree.BTree;

public class ArrayBackedRow extends AbstractRow {
   public static final ColumnData[] EMPTY_ARRAY = new ColumnData[0];
   private static final long EMPTY_SIZE;
   private final ColumnData[] data;
   private final int limit;

   private ArrayBackedRow(Clustering clustering, LivenessInfo primaryKeyLivenessInfo, Row.Deletion deletion, ColumnData[] data, int limit, int minLocalDeletionTime) {
      super(clustering, primaryKeyLivenessInfo, deletion, minLocalDeletionTime);
      this.data = data;
      this.limit = limit;
   }

   private ArrayBackedRow(Clustering clustering, ColumnData[] data, int minLocalDeletionTime) {
      this(clustering, LivenessInfo.EMPTY, Row.Deletion.LIVE, data, data.length, minLocalDeletionTime);
   }

   public static ArrayBackedRow create(Clustering clustering, LivenessInfo primaryKeyLivenessInfo, Row.Deletion deletion, ColumnData[] data, int limit) {
      int minDeletionTime = Math.min(minDeletionTime(primaryKeyLivenessInfo), minDeletionTime(deletion.time()));
      if(minDeletionTime != -2147483648) {
         for(int i = 0; i < limit; ++i) {
            ColumnData cd = data[i];
            minDeletionTime = Math.min(minDeletionTime, minDeletionTime(cd));
         }
      }

      return new ArrayBackedRow(clustering, primaryKeyLivenessInfo, deletion, data, limit, minDeletionTime);
   }

   public static ArrayBackedRow emptyRow(Clustering clustering) {
      return new ArrayBackedRow(clustering, EMPTY_ARRAY, 2147483647);
   }

   public static ArrayBackedRow singleCellRow(Clustering clustering, Cell cell) {
      if(cell.column().isSimple()) {
         return new ArrayBackedRow(clustering, new ColumnData[]{cell}, minDeletionTime(cell));
      } else {
         ComplexColumnData complexData = new ComplexColumnData(cell.column(), new Cell[]{cell}, DeletionTime.LIVE);
         return new ArrayBackedRow(clustering, new ColumnData[]{complexData}, minDeletionTime(cell));
      }
   }

   public static ArrayBackedRow emptyDeletedRow(Clustering clustering, Row.Deletion deletion) {
      assert !deletion.isLive();

      return new ArrayBackedRow(clustering, LivenessInfo.EMPTY, deletion, EMPTY_ARRAY, 0, -2147483648);
   }

   public static ArrayBackedRow noCellLiveRow(Clustering clustering, LivenessInfo primaryKeyLivenessInfo) {
      assert !primaryKeyLivenessInfo.isEmpty();

      return new ArrayBackedRow(clustering, primaryKeyLivenessInfo, Row.Deletion.LIVE, EMPTY_ARRAY, 0, minDeletionTime(primaryKeyLivenessInfo));
   }

   public Iterator<ColumnData> iterator() {
      return new AbstractIndexedListIterator<ColumnData>(this.limit, 0) {
         protected ColumnData get(int index) {
            return ArrayBackedRow.this.data[index];
         }
      };
   }

   public int size() {
      return this.limit;
   }

   public void setValue(ColumnMetadata column, CellPath path, ByteBuffer value) {
      int idx = Arrays.binarySearch(this.data, 0, this.limit, column, ColumnMetadata.asymmetricColumnDataComparator);
      if(idx >= 0) {
         ColumnData current = this.data[idx];
         if(column.isSimple()) {
            this.data[idx] = ((Cell)current).withUpdatedValue(value);
         } else {
            ((ComplexColumnData)current).setValue(path, value);
         }

      }
   }

   public boolean isEmpty() {
      return this.primaryKeyLivenessInfo().isEmpty() && this.deletion().isLive() && this.limit == 0;
   }

   public Cell getCell(ColumnMetadata cm) {
      assert cm.isSimple();

      int idx = Arrays.binarySearch(this.data, 0, this.limit, cm, ColumnMetadata.asymmetricColumnDataComparator);
      return idx < 0?null:(Cell)this.data[idx];
   }

   public Cell getCell(ColumnMetadata cm, CellPath path) {
      assert cm.isComplex();

      ComplexColumnData cd = this.getComplexColumnData(cm);
      return cd == null?null:cd.getCell(path);
   }

   public ComplexColumnData getComplexColumnData(ColumnMetadata cm) {
      assert cm.isComplex();

      int idx = Arrays.binarySearch(this.data, 0, this.limit, cm, ColumnMetadata.asymmetricColumnDataComparator);
      return idx < 0?null:(ComplexColumnData)this.data[idx];
   }

   public Iterable<Cell> cells() {
      return () -> {
         return new ArrayBackedRow.CellIterator();
      };
   }

   public Iterable<Cell> cellsInLegacyOrder(TableMetadata metadata, boolean reversed) {
      return () -> {
         return new ArrayBackedRow.CellInLegacyOrderIterator(metadata, reversed);
      };
   }

   public boolean hasComplexDeletion() {
      for(int i = this.limit - 1; i >= 0; --i) {
         ColumnData cd = this.data[i];
         if(cd.column.isSimple()) {
            break;
         }

         ComplexColumnData ccd = (ComplexColumnData)cd;
         if(!ccd.complexDeletion().isLive()) {
            return true;
         }
      }

      return false;
   }

   public boolean hasComplex() {
      int i = this.limit - 1;
      if(i >= 0) {
         ColumnData cd = this.data[i];
         if(!cd.column.isSimple()) {
            return true;
         }
      }

      return false;
   }

   static int minDeletionTime(Object[] data, int limit, LivenessInfo info, DeletionTime rowDeletion) {
      int min = Math.min(minDeletionTime(info), minDeletionTime(rowDeletion));

      for(int i = 0; i < limit; ++i) {
         min = Math.min(min, minDeletionTime((ColumnData)data[i]));
         if(min == -2147483648) {
            break;
         }
      }

      return min;
   }

   Row transformAndFilter(LivenessInfo info, Row.Deletion deletion, Function<ColumnData, ColumnData> function) {
      ColumnData[] transformed = this.data;
      int tlimit = 0;

      int minDeletionTime;
      for(minDeletionTime = 0; minDeletionTime < this.limit; ++minDeletionTime) {
         ColumnData cd1 = this.data[minDeletionTime];
         ColumnData cd2 = (ColumnData)function.apply(cd1);
         if(transformed == this.data && cd1 != cd2) {
            transformed = new ColumnData[this.limit];
            System.arraycopy(this.data, 0, transformed, 0, minDeletionTime);
         }

         if(cd2 != null) {
            transformed[tlimit++] = cd2;
         }
      }

      if(this.data == transformed && info == this.primaryKeyLivenessInfo && deletion == this.deletion) {
         return this;
      } else if(info.isEmpty() && deletion.isLive() && tlimit == 0) {
         return null;
      } else {
         minDeletionTime = minDeletionTime(transformed, tlimit, info, deletion.time());
         return new ArrayBackedRow(this.clustering, info, deletion, transformed, tlimit, minDeletionTime);
      }
   }

   public int dataSize() {
      int dataSize = this.clustering.dataSize() + this.primaryKeyLivenessInfo.dataSize() + this.deletion.dataSize();

      for(int i = 0; i < this.limit; ++i) {
         ColumnData c = this.data[i];
         dataSize += c.dataSize();
      }

      return dataSize;
   }

   public long unsharedHeapSizeExcludingData() {
      long heapSize = EMPTY_SIZE + this.clustering.unsharedHeapSizeExcludingData() + ObjectSizes.sizeOfArray((Object[])this.data);

      for(int i = 0; i < this.limit; ++i) {
         ColumnData c = this.data[i];
         heapSize += c.unsharedHeapSizeExcludingData();
      }

      return heapSize;
   }

   public Row withRowDeletion(DeletionTime newDeletion) {
      return !newDeletion.isLive() && this.deletion.isLive()?new ArrayBackedRow(this.clustering, this.primaryKeyLivenessInfo, Row.Deletion.regular(newDeletion), this.data, this.limit, -2147483648):this;
   }

   public void apply(Consumer<ColumnData> function, boolean reverse) {
      this.apply(function, (Predicate)null, reverse);
   }

   public void apply(Consumer<ColumnData> function, Predicate<ColumnData> stopCondition, boolean reverse) {
      if(!reverse) {
         this.applyForwards(function, stopCondition);
      } else {
         this.applyReversed(function, stopCondition);
      }

   }

   private boolean applyForwards(Consumer<ColumnData> function, Predicate<ColumnData> stopCondition) {
      for(int i = 0; i < this.limit; ++i) {
         ColumnData cd = this.data[i];
         function.accept(cd);
         if(stopCondition != null && stopCondition.apply(cd)) {
            return true;
         }
      }

      return false;
   }

   private boolean applyReversed(Consumer<ColumnData> function, Predicate<ColumnData> stopCondition) {
      for(int i = this.limit - 1; i >= 0; --i) {
         ColumnData cd = this.data[i];
         function.accept(cd);
         if(stopCondition != null && stopCondition.apply(cd)) {
            return true;
         }
      }

      return false;
   }

   public <R> R reduce(R seed, BTree.ReduceFunction<R, ColumnData> reducer) {
      for(int i = 0; i < this.limit; ++i) {
         seed = reducer.apply(seed, this.data[i]);
         if(reducer.stop(seed)) {
            break;
         }
      }

      return seed;
   }

   public <R> R reduceCells(R seed, final BTree.ReduceFunction<R, Cell> reducer) {
      return this.reduce(seed, new BTree.ReduceFunction<R, ColumnData>() {
         public R apply(R ret, ColumnData cd) {
            return cd.column().isComplex()?((ComplexColumnData)cd).reduce(ret, reducer):reducer.apply(ret, (Cell)cd);
         }

         public boolean stop(R ret) {
            return reducer.stop(ret);
         }
      });
   }

   public static ArrayBackedRow.Builder sortedBuilder() {
      return new ArrayBackedRow.Builder(true, -2147483648);
   }

   public static ArrayBackedRow.Builder unsortedBuilder(int nowInSeconds) {
      return new ArrayBackedRow.Builder(false, nowInSeconds);
   }

   static {
      EMPTY_SIZE = ObjectSizes.measure(emptyRow(Clustering.EMPTY));
   }

   public static class Builder implements Row.Builder {
      protected Clustering clustering;
      protected LivenessInfo primaryKeyLivenessInfo;
      protected Row.Deletion deletion;
      private ColumnData[] cells;
      private int length;
      private int lastLength;
      private boolean hasComplex;
      private final boolean isSorted;
      private final int nowInSeconds;
      private BTree.Builder.Resolver resolver;

      protected Builder(boolean isSorted, int nowInSeconds, int size) {
         this.primaryKeyLivenessInfo = LivenessInfo.EMPTY;
         this.deletion = Row.Deletion.LIVE;
         this.cells = ArrayBackedRow.EMPTY_ARRAY;
         this.length = 0;
         this.lastLength = 0;
         this.hasComplex = false;
         this.resolver = null;
         this.isSorted = isSorted;
         this.nowInSeconds = nowInSeconds;
         this.cells = new ColumnData[size];
      }

      protected Builder(boolean isSorted, int nowInSeconds) {
         this.primaryKeyLivenessInfo = LivenessInfo.EMPTY;
         this.deletion = Row.Deletion.LIVE;
         this.cells = ArrayBackedRow.EMPTY_ARRAY;
         this.length = 0;
         this.lastLength = 0;
         this.hasComplex = false;
         this.resolver = null;
         this.isSorted = isSorted;
         this.nowInSeconds = nowInSeconds;
      }

      public Row.Builder copy() {
         ArrayBackedRow.Builder copy = new ArrayBackedRow.Builder(this.isSorted, this.nowInSeconds);
         copy.deletion = this.deletion;
         copy.length = this.length;
         copy.clustering = this.clustering;
         copy.primaryKeyLivenessInfo = this.primaryKeyLivenessInfo;
         copy.cells = (ColumnData[])Arrays.copyOf(this.cells, this.cells.length);
         copy.lastLength = this.lastLength;
         copy.hasComplex = this.hasComplex;
         return copy;
      }

      public boolean isSorted() {
         return this.isSorted;
      }

      public void newRow(Clustering clustering) {
         assert this.clustering == null;

         this.clustering = clustering;
      }

      public Clustering clustering() {
         return this.clustering;
      }

      public void addPrimaryKeyLivenessInfo(LivenessInfo info) {
         if(!this.deletion.deletes(info)) {
            this.primaryKeyLivenessInfo = info;
         }

      }

      public void addRowDeletion(Row.Deletion deletion) {
         this.deletion = deletion;
         if(deletion.deletes(this.primaryKeyLivenessInfo)) {
            this.primaryKeyLivenessInfo = LivenessInfo.EMPTY;
         }

      }

      void maybeGrow() {
         if(this.length == this.cells.length) {
            ColumnData[] newCells;
            if(this.lastLength > 0) {
               newCells = new ColumnData[this.lastLength];
               this.lastLength = 0;
            } else {
               newCells = new ColumnData[Math.max(8, this.length * 2)];
            }

            System.arraycopy(this.cells, 0, newCells, 0, this.length);
            this.cells = newCells;
         }

      }

      public void addCell(Cell cell) {
         assert cell.column().isStatic() == (this.clustering == Clustering.STATIC_CLUSTERING) : "Column is " + cell.column() + ", clustering = " + this.clustering;

         if(!this.deletion.deletes(cell)) {
            this.maybeGrow();
            this.cells[this.length++] = cell;
            this.hasComplex |= cell.column.isComplex();
         }
      }

      public void addComplexDeletion(ColumnMetadata column, DeletionTime complexDeletion) {
         this.maybeGrow();
         this.cells[this.length++] = new AbstractRow.ComplexColumnDeletion(column, complexDeletion);
         this.hasComplex = true;
      }

      public void reset() {
         this.clustering = null;
         this.primaryKeyLivenessInfo = LivenessInfo.EMPTY;
         this.deletion = Row.Deletion.LIVE;
         this.cells = ArrayBackedRow.EMPTY_ARRAY;
         this.lastLength = this.length;
         this.length = 0;
         this.hasComplex = false;
      }

      public void resolve() {
         if(this.length > 0) {
            if(this.resolver == null) {
               this.resolver = new AbstractRow.CellResolver(this.nowInSeconds);
            }

            int c = 0;
            int prev = 0;

            for(int i = 1; i < this.length; ++i) {
               if(ColumnData.comparator.compare(this.cells[i], this.cells[prev]) != 0) {
                  this.cells[c++] = (ColumnData)this.resolver.resolve(this.cells, prev, i);
                  prev = i;
               }
            }

            this.cells[c++] = (ColumnData)this.resolver.resolve(this.cells, prev, this.length);
            this.length = c;
         }

      }

      public Row build() {
         if(!this.isSorted) {
            Arrays.sort(this.cells, 0, this.length, ColumnData.comparator);
         }

         if(!this.isSorted | this.hasComplex) {
            this.resolve();
         }

         if(this.deletion.isShadowedBy(this.primaryKeyLivenessInfo)) {
            this.deletion = Row.Deletion.LIVE;
         }

         Row result = ArrayBackedRow.create(this.clustering, this.primaryKeyLivenessInfo, this.deletion, this.cells, this.length);
         this.reset();
         return result;
      }
   }

   private class CellInLegacyOrderIterator extends AbstractIterator<Cell> {
      private final Comparator<ByteBuffer> comparator;
      private final boolean reversed;
      private final int firstComplexIdx;
      private int simpleIdx;
      private int complexIdx;
      private Iterator<Cell> complexCells;

      private CellInLegacyOrderIterator(TableMetadata metadata, boolean reversed) {
         AbstractType<?> nameComparator = metadata.columnDefinitionNameComparator(ArrayBackedRow.this.isStatic()?ColumnMetadata.Kind.STATIC:ColumnMetadata.Kind.REGULAR);
         this.comparator = (Comparator)(reversed?Collections.reverseOrder(nameComparator):nameComparator);
         this.reversed = reversed;
         int idx = Iterators.indexOf(Iterators.forArray(ArrayBackedRow.this.data), (cd) -> {
            return cd instanceof ComplexColumnData;
         });
         this.firstComplexIdx = idx < 0?ArrayBackedRow.this.limit:idx;
         this.complexIdx = this.firstComplexIdx;
      }

      private int getSimpleIdx() {
         return this.reversed?this.firstComplexIdx - this.simpleIdx - 1:this.simpleIdx;
      }

      private int getSimpleIdxAndIncrement() {
         int idx = this.getSimpleIdx();
         ++this.simpleIdx;
         return idx;
      }

      private int getComplexIdx() {
         return this.reversed?ArrayBackedRow.this.limit + this.firstComplexIdx - this.complexIdx - 1:this.complexIdx;
      }

      private int getComplexIdxAndIncrement() {
         int idx = this.getComplexIdx();
         ++this.complexIdx;
         return idx;
      }

      private Iterator<Cell> makeComplexIterator(Object complexData) {
         ComplexColumnData ccd = (ComplexColumnData)complexData;
         return this.reversed?ccd.reverseIterator():ccd.iterator();
      }

      protected Cell computeNext() {
         while(true) {
            if(this.complexCells != null) {
               if(this.complexCells.hasNext()) {
                  return (Cell)this.complexCells.next();
               }

               this.complexCells = null;
            }

            if(this.simpleIdx >= this.firstComplexIdx) {
               if(this.complexIdx >= ArrayBackedRow.this.limit) {
                  return (Cell)this.endOfData();
               }

               this.complexCells = this.makeComplexIterator(ArrayBackedRow.this.data[this.getComplexIdxAndIncrement()]);
            } else {
               if(this.complexIdx >= ArrayBackedRow.this.limit) {
                  return (Cell)ArrayBackedRow.this.data[this.getSimpleIdxAndIncrement()];
               }

               if(this.comparator.compare(ArrayBackedRow.this.data[this.getSimpleIdx()].column().name.bytes, ArrayBackedRow.this.data[this.getComplexIdx()].column().name.bytes) < 0) {
                  return (Cell)ArrayBackedRow.this.data[this.getSimpleIdxAndIncrement()];
               }

               this.complexCells = this.makeComplexIterator(ArrayBackedRow.this.data[this.getComplexIdxAndIncrement()]);
            }
         }
      }
   }

   private class CellIterator extends AbstractIterator<Cell> {
      private Iterator<ColumnData> columnData;
      private Iterator<Cell> complexCells;

      private CellIterator() {
         this.columnData = ArrayBackedRow.this.iterator();
      }

      protected Cell computeNext() {
         while(true) {
            if(this.complexCells != null) {
               if(this.complexCells.hasNext()) {
                  return (Cell)this.complexCells.next();
               }

               this.complexCells = null;
            }

            if(!this.columnData.hasNext()) {
               return (Cell)this.endOfData();
            }

            ColumnData cd = (ColumnData)this.columnData.next();
            if(!cd.column().isComplex()) {
               return (Cell)cd;
            }

            this.complexCells = ((ComplexColumnData)cd).iterator();
         }
      }
   }
}
