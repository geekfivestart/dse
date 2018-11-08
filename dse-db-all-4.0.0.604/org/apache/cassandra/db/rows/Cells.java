package org.apache.cassandra.db.rows;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import org.apache.cassandra.db.Conflicts;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.partitions.PartitionStatisticsCollector;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.NumberUtil;

public abstract class Cells {
   private Cells() {
   }

   public static void collectStats(Cell cell, PartitionStatisticsCollector collector) {
      collector.update(cell);
      if(cell.isCounterCell()) {
         collector.updateHasLegacyCounterShards(CounterCells.hasLegacyShards(cell));
      }

   }

   public static long reconcile(Cell existing, Cell update, DeletionTime deletion, Row.Builder builder, int nowInSec) {
      existing = existing != null && !deletion.deletes(existing)?existing:null;
      update = update != null && !deletion.deletes(update)?update:null;
      if(existing != null && update != null) {
         Cell reconciled = reconcile(existing, update, nowInSec);
         builder.addCell(reconciled);
         return NumberUtil.consistentAbs(existing.timestamp() - update.timestamp());
      } else {
         if(update != null) {
            builder.addCell(update);
         } else if(existing != null) {
            builder.addCell(existing);
         }

         return 9223372036854775807L;
      }
   }

   public static Cell reconcile(Cell c1, Cell c2, int nowInSec) {
      if(c1 == null) {
         return c2 == null?null:c2;
      } else if(c2 == null) {
         return c1;
      } else {
         Conflicts.Resolution res;
         if(!c1.isCounterCell() && !c2.isCounterCell()) {
            res = Conflicts.resolveRegular(c1.timestamp(), c1.isLive(nowInSec), c1.localDeletionTime(), c1.value(), c2.timestamp(), c2.isLive(nowInSec), c2.localDeletionTime(), c2.value());

            assert res != Conflicts.Resolution.MERGE;

            return res == Conflicts.Resolution.LEFT_WINS?c1:c2;
         } else {
            res = Conflicts.resolveCounter(c1.timestamp(), c1.isLive(nowInSec), c1.value(), c2.timestamp(), c2.isLive(nowInSec), c2.value());
            switch(null.$SwitchMap$org$apache$cassandra$db$Conflicts$Resolution[res.ordinal()]) {
            case 1:
               return c1;
            case 2:
               return c2;
            default:
               ByteBuffer merged = Conflicts.mergeCounterValues(c1.value(), c2.value());
               long timestamp = Math.max(c1.timestamp(), c2.timestamp());
               return (Cell)(merged == c1.value() && timestamp == c1.timestamp()?c1:(merged == c2.value() && timestamp == c2.timestamp()?c2:new BufferCell(c1.column(), timestamp, 0, 2147483647, merged, c1.path())));
            }
         }
      }
   }

   public static long reconcileComplex(ColumnMetadata column, Iterator<Cell> existing, Iterator<Cell> update, DeletionTime deletion, Row.Builder builder, int nowInSec) {
      Comparator<CellPath> comparator = column.cellPathComparator();
      Cell nextExisting = getNext(existing);
      Cell nextUpdate = getNext(update);
      long timeDelta = 9223372036854775807L;

      while(nextExisting != null || nextUpdate != null) {
         int cmp = nextExisting == null?1:(nextUpdate == null?-1:comparator.compare(nextExisting.path(), nextUpdate.path()));
         if(cmp < 0) {
            reconcile(nextExisting, (Cell)null, deletion, builder, nowInSec);
            nextExisting = getNext(existing);
         } else if(cmp > 0) {
            reconcile((Cell)null, nextUpdate, deletion, builder, nowInSec);
            nextUpdate = getNext(update);
         } else {
            timeDelta = Math.min(timeDelta, reconcile(nextExisting, nextUpdate, deletion, builder, nowInSec));
            nextExisting = getNext(existing);
            nextUpdate = getNext(update);
         }
      }

      return timeDelta;
   }

   public static void addNonShadowed(Cell existing, Cell update, DeletionTime deletion, Row.Builder builder, int nowInSec) {
      if(!deletion.deletes(existing)) {
         Cell reconciled = reconcile(existing, update, nowInSec);
         if(reconciled != update) {
            builder.addCell(existing);
         }

      }
   }

   public static void addNonShadowedComplex(ColumnMetadata column, Iterator<Cell> existing, Iterator<Cell> update, DeletionTime deletion, Row.Builder builder, int nowInSec) {
      Comparator<CellPath> comparator = column.cellPathComparator();
      Cell nextExisting = getNext(existing);
      Cell nextUpdate = getNext(update);

      while(nextExisting != null) {
         int cmp = nextUpdate == null?-1:comparator.compare(nextExisting.path(), nextUpdate.path());
         if(cmp < 0) {
            addNonShadowed(nextExisting, (Cell)null, deletion, builder, nowInSec);
            nextExisting = getNext(existing);
         } else if(cmp == 0) {
            addNonShadowed(nextExisting, nextUpdate, deletion, builder, nowInSec);
            nextExisting = getNext(existing);
            nextUpdate = getNext(update);
         } else {
            nextUpdate = getNext(update);
         }
      }

   }

   private static Cell getNext(Iterator<Cell> iterator) {
      return iterator != null && iterator.hasNext()?(Cell)iterator.next():null;
   }
}
