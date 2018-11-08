package org.apache.cassandra.db.rows;

import com.google.common.base.Function;
import com.google.common.hash.Hasher;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.DroppedColumn;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.btree.BTree;

public class ComplexColumnData extends ColumnData implements Iterable<Cell> {
   static final Cell[] NO_CELLS = new Cell[0];
   private static final long EMPTY_SIZE;
   private final Object[] cells;
   private final DeletionTime complexDeletion;

   ComplexColumnData(ColumnMetadata column, Object[] cells, DeletionTime complexDeletion) {
      super(column);

      assert column.isComplex();

      assert cells.length > 0 || !complexDeletion.isLive();

      this.cells = cells;
      this.complexDeletion = complexDeletion;
   }

   public boolean hasCells() {
      return !BTree.isEmpty(this.cells);
   }

   public int cellsCount() {
      return BTree.size(this.cells);
   }

   public Cell getCell(CellPath path) {
      return (Cell)BTree.find(this.cells, this.column.asymmetricCellPathComparator(), path);
   }

   public <R> R reduce(R seed, BTree.ReduceFunction<R, Cell> reducer) {
      return BTree.reduce(this.cells, seed, reducer);
   }

   public Cell getCellByIndex(int idx) {
      return (Cell)BTree.findByIndex(this.cells, idx);
   }

   public DeletionTime complexDeletion() {
      return this.complexDeletion;
   }

   public Iterator<Cell> iterator() {
      return BTree.iterator(this.cells);
   }

   public Iterator<Cell> reverseIterator() {
      return BTree.iterator(this.cells, BTree.Dir.DESC);
   }

   public void applyForwards(Consumer<Cell> function) {
      BTree.apply(this.cells, function, false);
   }

   public int dataSize() {
      int size = this.complexDeletion.dataSize();

      Cell cell;
      for(Iterator var2 = this.iterator(); var2.hasNext(); size += cell.dataSize()) {
         cell = (Cell)var2.next();
      }

      return size;
   }

   public long unsharedHeapSizeExcludingData() {
      long heapSize = EMPTY_SIZE + ObjectSizes.sizeOfArray(this.cells);

      Cell cell;
      for(Iterator var3 = this.iterator(); var3.hasNext(); heapSize += cell.unsharedHeapSizeExcludingData()) {
         cell = (Cell)var3.next();
      }

      return heapSize;
   }

   public void validate() {
      Iterator var1 = this.iterator();

      while(var1.hasNext()) {
         Cell cell = (Cell)var1.next();
         cell.validate();
      }

   }

   public void digest(Hasher hasher) {
      if(!this.complexDeletion.isLive()) {
         this.complexDeletion.digest(hasher);
      }

      Iterator var2 = this.iterator();

      while(var2.hasNext()) {
         Cell cell = (Cell)var2.next();
         cell.digest(hasher);
      }

   }

   public ComplexColumnData markCounterLocalToBeCleared() {
      return this.transformAndFilter(this.complexDeletion, Cell::markCounterLocalToBeCleared);
   }

   public ComplexColumnData filter(ColumnFilter filter, DeletionTime activeDeletion, DroppedColumn dropped, LivenessInfo rowLiveness) {
      ColumnFilter.Tester cellTester = filter.newTester(this.column);
      if(cellTester == null && activeDeletion.isLive() && dropped == null && filter.fetchedColumnIsQueried(this.column)) {
         return this;
      } else {
         DeletionTime newDeletion = activeDeletion.supersedes(this.complexDeletion)?DeletionTime.LIVE:this.complexDeletion;
         return this.transformAndFilter(newDeletion, (cell) -> {
            CellPath path = cell.path();
            boolean isForDropped = dropped != null && cell.timestamp() <= dropped.droppedTime;
            boolean isShadowed = activeDeletion.deletes(cell);
            boolean isSkippable = cellTester != null && (!cellTester.fetches(path) || !cellTester.fetchedCellIsQueried(path) && cell.timestamp() < rowLiveness.timestamp());
            if(!isForDropped && !isShadowed && !isSkippable) {
               boolean shouldSkipValue = !filter.fetchedColumnIsQueried(this.column) || path != null && cellTester != null && !cellTester.fetchedCellIsQueried(path);
               return shouldSkipValue?cell.withSkippedValue():cell;
            } else {
               return null;
            }
         });
      }
   }

   public ComplexColumnData purge(DeletionPurger purger, int nowInSec) {
      DeletionTime newDeletion = !this.complexDeletion.isLive() && !purger.shouldPurge(this.complexDeletion)?this.complexDeletion:DeletionTime.LIVE;
      return this.transformAndFilter(newDeletion, (cell) -> {
         return cell.purge(purger, nowInSec);
      });
   }

   public ComplexColumnData withOnlyQueriedData(ColumnFilter filter) {
      return this.transformAndFilter(this.complexDeletion, (cell) -> {
         return filter.fetchedCellIsQueried(this.column, cell.path())?null:cell;
      });
   }

   private ComplexColumnData transformAndFilter(DeletionTime newDeletion, Function<? super Cell, ? extends Cell> function) {
      Object[] transformed = BTree.transformAndFilter(this.cells, function);
      return this.cells == transformed && newDeletion == this.complexDeletion?this:(newDeletion == DeletionTime.LIVE && BTree.isEmpty(transformed)?null:new ComplexColumnData(this.column, transformed, newDeletion));
   }

   public ComplexColumnData updateAllTimestamp(long newTimestamp) {
      DeletionTime newDeletion = this.complexDeletion.isLive()?this.complexDeletion:new DeletionTime(newTimestamp - 1L, this.complexDeletion.localDeletionTime());
      return this.transformAndFilter(newDeletion, (cell) -> {
         return (Cell)cell.updateAllTimestamp(newTimestamp);
      });
   }

   public long maxTimestamp() {
      long timestamp = this.complexDeletion.markedForDeleteAt();

      Cell cell;
      for(Iterator var3 = this.iterator(); var3.hasNext(); timestamp = Math.max(timestamp, cell.timestamp())) {
         cell = (Cell)var3.next();
      }

      return timestamp;
   }

   void setValue(CellPath path, ByteBuffer value) {
      Cell current = (Cell)BTree.find(this.cells, this.column.asymmetricCellPathComparator(), path);
      BTree.replaceInSitu(this.cells, this.column.cellComparator(), current, current.withUpdatedValue(value));
   }

   public boolean equals(Object other) {
      if(this == other) {
         return true;
      } else if(!(other instanceof ComplexColumnData)) {
         return false;
      } else {
         ComplexColumnData that = (ComplexColumnData)other;
         return this.column().equals(that.column()) && this.complexDeletion().equals(that.complexDeletion) && BTree.equals(this.cells, that.cells);
      }
   }

   public String toString() {
      StringBuilder b = new StringBuilder();
      Iterator var2 = BTree.iterable(this.cells).iterator();

      while(var2.hasNext()) {
         Object c = var2.next();
         b.append(c.toString());
      }

      b.append(String.format("[%s=<tombstone> %s]", new Object[]{this.column().name, this.livenessInfoString()}));
      return b.toString();
   }

   private String livenessInfoString() {
      return this.complexDeletion != null?String.format("ldt=%d", new Object[]{Integer.valueOf(this.complexDeletion.localDeletionTime())}):"";
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.column(), this.complexDeletion(), Integer.valueOf(BTree.hashCode(this.cells))});
   }

   public static ComplexColumnData.Builder builder() {
      return new ComplexColumnData.Builder();
   }

   static {
      EMPTY_SIZE = ObjectSizes.measure(new ComplexColumnData(ColumnMetadata.regularColumn("", "", "", SetType.getInstance(ByteType.instance, true)), NO_CELLS, new DeletionTime(0L, 0)));
   }

   public static class Builder {
      private DeletionTime complexDeletion;
      private ColumnMetadata column;
      private BTree.Builder<Cell> builder;

      public Builder() {
      }

      public void newColumn(ColumnMetadata column) {
         this.column = column;
         this.complexDeletion = DeletionTime.LIVE;
         this.builder = BTree.builder(column.cellComparator());
      }

      public void addComplexDeletion(DeletionTime complexDeletion) {
         this.complexDeletion = complexDeletion;
      }

      public void addCell(Cell cell) {
         this.builder.add(cell);
      }

      public ComplexColumnData build() {
         return this.complexDeletion.isLive() && this.builder.isEmpty()?null:new ComplexColumnData(this.column, this.builder.build(), this.complexDeletion);
      }
   }
}
