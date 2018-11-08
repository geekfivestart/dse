package org.apache.cassandra.db.rows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.memory.AbstractAllocator;

public abstract class Cell extends ColumnData {
   public static final int NO_TTL = 0;
   public static final int NO_DELETION_TIME = 2147483647;
   public static final int MAX_DELETION_TIME = 2147483646;
   public static final Comparator<Cell> comparator = (c1, c2) -> {
      int cmp = c1.column().compareTo(c2.column());
      if(cmp != 0) {
         return cmp;
      } else {
         Comparator<CellPath> pathComparator = c1.column().cellPathComparator();
         return pathComparator == null?0:pathComparator.compare(c1.path(), c2.path());
      }
   };
   public static final Cell.Serializer serializer = new Cell.Serializer();

   protected Cell(ColumnMetadata column) {
      super(column);
   }

   public abstract boolean isCounterCell();

   public abstract ByteBuffer value();

   public abstract long timestamp();

   public abstract int ttl();

   public abstract int localDeletionTime();

   public abstract boolean isTombstone();

   public abstract boolean isExpiring();

   public abstract boolean isLive(int var1);

   public abstract CellPath path();

   public abstract Cell withUpdatedColumn(ColumnMetadata var1);

   public abstract Cell withUpdatedValue(ByteBuffer var1);

   public abstract Cell withUpdatedTimestampAndLocalDeletionTime(long var1, int var3);

   public abstract Cell withSkippedValue();

   public abstract Cell copy(AbstractAllocator var1);

   public abstract Cell markCounterLocalToBeCleared();

   public abstract Cell purge(DeletionPurger var1, int var2);

   static class Serializer {
      private static final int IS_DELETED_MASK = 1;
      private static final int IS_EXPIRING_MASK = 2;
      private static final int HAS_EMPTY_VALUE_MASK = 4;
      private static final int USE_ROW_TIMESTAMP_MASK = 8;
      private static final int USE_ROW_TTL_MASK = 16;

      Serializer() {
      }

      public void serialize(Cell cell, ColumnMetadata column, DataOutputPlus out, LivenessInfo rowLiveness, SerializationHeader header) throws IOException {
         assert cell != null;

         boolean hasValue = cell.value().hasRemaining();
         boolean isDeleted = cell.isTombstone();
         boolean isExpiring = cell.isExpiring();
         boolean useRowTimestamp = !rowLiveness.isEmpty() && cell.timestamp() == rowLiveness.timestamp();
         boolean useRowTTL = isExpiring && rowLiveness.isExpiring() && cell.ttl() == rowLiveness.ttl() && cell.localDeletionTime() == rowLiveness.localExpirationTime();
         int flags = 0;
         if(!hasValue) {
            flags |= 4;
         }

         if(isDeleted) {
            flags |= 1;
         } else if(isExpiring) {
            flags |= 2;
         }

         if(useRowTimestamp) {
            flags |= 8;
         }

         if(useRowTTL) {
            flags |= 16;
         }

         out.writeByte((byte)flags);
         if(!useRowTimestamp) {
            header.writeTimestamp(cell.timestamp(), out);
         }

         if((isDeleted || isExpiring) && !useRowTTL) {
            header.writeLocalDeletionTime(cell.localDeletionTime(), out);
         }

         if(isExpiring && !useRowTTL) {
            header.writeTTL(cell.ttl(), out);
         }

         if(column.isComplex()) {
            column.cellPathSerializer().serialize(cell.path(), out);
         }

         if(hasValue) {
            header.getType(column).writeValue(cell.value(), out);
         }

      }

      public Cell deserialize(DataInputPlus in, LivenessInfo rowLiveness, ColumnMetadata column, SerializationHeader header, SerializationHelper helper) throws IOException {
         int flags = in.readUnsignedByte();
         boolean hasValue = (flags & 4) == 0;
         boolean isDeleted = (flags & 1) != 0;
         boolean isExpiring = (flags & 2) != 0;
         boolean useRowTimestamp = (flags & 8) != 0;
         boolean useRowTTL = (flags & 16) != 0;
         long timestamp = useRowTimestamp?rowLiveness.timestamp():header.readTimestamp(in);
         int localDeletionTime = useRowTTL?rowLiveness.localExpirationTime():(!isDeleted && !isExpiring?2147483647:header.readLocalDeletionTime(in));
         int ttl = useRowTTL?rowLiveness.ttl():(isExpiring?header.readTTL(in):0);
         CellPath path = column.isComplex()?column.cellPathSerializer().deserialize(in):null;
         ByteBuffer value = ByteBufferUtil.EMPTY_BYTE_BUFFER;
         if(hasValue) {
            if(helper.canSkipValue(column) || path != null && helper.canSkipValue(path)) {
               header.getType(column).skipValue(in);
            } else {
               boolean isCounter = localDeletionTime == 2147483647 && column.type.isCounter();
               value = header.getType(column).readValue(in, DatabaseDescriptor.getMaxValueSize());
               if(isCounter) {
                  value = helper.maybeClearCounterValue(value);
               }
            }
         }

         return new BufferCell(column, timestamp, ttl, localDeletionTime, value, path);
      }

      public long serializedSize(Cell cell, ColumnMetadata column, LivenessInfo rowLiveness, SerializationHeader header) {
         long size = 1L;
         boolean hasValue = cell.value().hasRemaining();
         boolean isDeleted = cell.isTombstone();
         boolean isExpiring = cell.isExpiring();
         boolean useRowTimestamp = !rowLiveness.isEmpty() && cell.timestamp() == rowLiveness.timestamp();
         boolean useRowTTL = isExpiring && rowLiveness.isExpiring() && cell.ttl() == rowLiveness.ttl() && cell.localDeletionTime() == rowLiveness.localExpirationTime();
         if(!useRowTimestamp) {
            size += header.timestampSerializedSize(cell.timestamp());
         }

         if((isDeleted || isExpiring) && !useRowTTL) {
            size += header.localDeletionTimeSerializedSize(cell.localDeletionTime());
         }

         if(isExpiring && !useRowTTL) {
            size += header.ttlSerializedSize(cell.ttl());
         }

         if(column.isComplex()) {
            size += column.cellPathSerializer().serializedSize(cell.path());
         }

         if(hasValue) {
            size += (long)header.getType(column).writtenLength(cell.value());
         }

         return size;
      }

      public boolean skip(DataInputPlus in, ColumnMetadata column, SerializationHeader header) throws IOException {
         int flags = in.readUnsignedByte();
         boolean hasValue = (flags & 4) == 0;
         boolean isDeleted = (flags & 1) != 0;
         boolean isExpiring = (flags & 2) != 0;
         boolean useRowTimestamp = (flags & 8) != 0;
         boolean useRowTTL = (flags & 16) != 0;
         if(!useRowTimestamp) {
            header.skipTimestamp(in);
         }

         if(!useRowTTL && (isDeleted || isExpiring)) {
            header.skipLocalDeletionTime(in);
         }

         if(!useRowTTL && isExpiring) {
            header.skipTTL(in);
         }

         if(column.isComplex()) {
            column.cellPathSerializer().skip(in);
         }

         if(hasValue) {
            header.getType(column).skipValue(in);
         }

         return true;
      }
   }
}
