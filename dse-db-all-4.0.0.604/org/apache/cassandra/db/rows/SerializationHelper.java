package org.apache.cassandra.db.rows;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.EncodingVersion;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.DroppedColumn;
import org.apache.cassandra.schema.TableMetadata;

public class SerializationHelper {
   private final SerializationHelper.Flag flag;
   public final EncodingVersion version;
   private final ColumnFilter columnsToFetch;
   private ColumnFilter.Tester tester;
   private final Map<ByteBuffer, DroppedColumn> droppedColumns;
   private DroppedColumn currentDroppedComplex;

   public SerializationHelper(TableMetadata metadata, EncodingVersion version, SerializationHelper.Flag flag, ColumnFilter columnsToFetch) {
      this.flag = flag;
      this.version = version;
      this.columnsToFetch = columnsToFetch == null?null:columnsToFetch.withPartitionColumnsVerified(metadata.regularAndStaticColumns());
      this.droppedColumns = metadata.droppedColumns;
   }

   public SerializationHelper(TableMetadata metadata, EncodingVersion version, SerializationHelper.Flag flag) {
      this(metadata, version, flag, (ColumnFilter)null);
   }

   public boolean includes(ColumnMetadata column) {
      return this.columnsToFetch == null || this.columnsToFetch.fetches(column);
   }

   public boolean includes(Cell cell, LivenessInfo rowLiveness) {
      if(this.columnsToFetch == null) {
         return true;
      } else {
         ColumnMetadata column = cell.column();
         return column.isComplex()?(!this.includes(cell.path())?false:!this.canSkipValue(cell.path()) || cell.timestamp() >= rowLiveness.timestamp()):this.columnsToFetch.fetchedColumnIsQueried(column) || cell.timestamp() >= rowLiveness.timestamp();
      }
   }

   public boolean includes(CellPath path) {
      return path == null || this.tester == null || this.tester.fetches(path);
   }

   public boolean canSkipValue(ColumnMetadata column) {
      return this.columnsToFetch != null && !this.columnsToFetch.fetchedColumnIsQueried(column);
   }

   public boolean canSkipValue(CellPath path) {
      return path != null && this.tester != null && !this.tester.fetchedCellIsQueried(path);
   }

   public void startOfComplexColumn(ColumnMetadata column) {
      this.tester = this.columnsToFetch == null?null:this.columnsToFetch.newTester(column);
      this.currentDroppedComplex = (DroppedColumn)this.droppedColumns.get(column.name.bytes);
   }

   public void endOfComplexColumn() {
      this.tester = null;
   }

   public boolean isDropped(Cell cell, boolean isComplex) {
      if(this.droppedColumns.isEmpty()) {
         return false;
      } else {
         DroppedColumn dropped = isComplex?this.currentDroppedComplex:(DroppedColumn)this.droppedColumns.get(cell.column().name.bytes);
         return dropped != null && cell.timestamp() <= dropped.droppedTime;
      }
   }

   public boolean isDroppedComplexDeletion(DeletionTime complexDeletion) {
      return this.currentDroppedComplex != null && complexDeletion.markedForDeleteAt() <= this.currentDroppedComplex.droppedTime;
   }

   public ByteBuffer maybeClearCounterValue(ByteBuffer value) {
      return this.flag != SerializationHelper.Flag.FROM_REMOTE && (this.flag != SerializationHelper.Flag.LOCAL || !CounterContext.instance().shouldClearLocal(value))?value:CounterContext.instance().clearAllLocal(value);
   }

   public static enum Flag {
      LOCAL,
      FROM_REMOTE,
      PRESERVE_SIZE;

      private Flag() {
      }
   }
}
