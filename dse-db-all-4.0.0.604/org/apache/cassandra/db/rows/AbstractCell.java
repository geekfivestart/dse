package org.apache.cassandra.db.rows;

import com.google.common.hash.Hasher;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.HashingUtils;
import org.apache.cassandra.utils.memory.AbstractAllocator;

public abstract class AbstractCell extends Cell {
   protected AbstractCell(ColumnMetadata column) {
      super(column);
   }

   public boolean isCounterCell() {
      return !this.isTombstone() && this.column.isCounterColumn();
   }

   public boolean isLive(int nowInSec) {
      return this.localDeletionTime() == 2147483647 || this.ttl() != 0 && nowInSec < this.localDeletionTime();
   }

   public boolean isTombstone() {
      return this.localDeletionTime() != 2147483647 && this.ttl() == 0;
   }

   public boolean isExpiring() {
      return this.ttl() != 0;
   }

   public Cell markCounterLocalToBeCleared() {
      if(!this.isCounterCell()) {
         return this;
      } else {
         ByteBuffer value = this.value();
         ByteBuffer marked = CounterContext.instance().markLocalToBeCleared(value);
         return (Cell)(marked == value?this:new BufferCell(this.column, this.timestamp(), this.ttl(), this.localDeletionTime(), marked, this.path()));
      }
   }

   public Cell purge(DeletionPurger purger, int nowInSec) {
      if(!this.isLive(nowInSec)) {
         if(purger.shouldPurge(this.timestamp(), this.localDeletionTime())) {
            return null;
         }

         if(this.isExpiring()) {
            return BufferCell.tombstone(this.column, this.timestamp(), this.localDeletionTime() - this.ttl(), this.path()).purge(purger, nowInSec);
         }
      }

      return this;
   }

   public Cell copy(AbstractAllocator allocator) {
      CellPath path = this.path();
      return new BufferCell(this.column, this.timestamp(), this.ttl(), this.localDeletionTime(), allocator.clone(this.value()), path == null?null:path.copy(allocator));
   }

   public Cell updateAllTimestamp(long newTimestamp) {
      return new BufferCell(this.column, this.isTombstone()?newTimestamp - 1L:newTimestamp, this.ttl(), this.localDeletionTime(), this.value(), this.path());
   }

   public int dataSize() {
      CellPath path = this.path();
      return TypeSizes.sizeof(this.timestamp()) + TypeSizes.sizeof(this.ttl()) + TypeSizes.sizeof(this.localDeletionTime()) + this.value().remaining() + (path == null?0:path.dataSize());
   }

   public void digest(Hasher hasher) {
      if(this.isCounterCell()) {
         CounterContext.instance().updateDigest(hasher, this.value());
      } else {
         HashingUtils.updateBytes(hasher, this.value().duplicate());
      }

      HashingUtils.updateWithLong(hasher, this.timestamp());
      HashingUtils.updateWithInt(hasher, this.ttl());
      HashingUtils.updateWithBoolean(hasher, this.isCounterCell());
      if(this.path() != null) {
         this.path().digest(hasher);
      }

   }

   public void validate() {
      if(this.ttl() < 0) {
         throw new MarshalException("A TTL should not be negative");
      } else if(this.localDeletionTime() < 0) {
         throw new MarshalException("A local deletion time should not be negative");
      } else if(this.isExpiring() && this.localDeletionTime() == 2147483647) {
         throw new MarshalException("Shoud not have a TTL without an associated local deletion time");
      } else {
         this.column().validateCell(this);
      }
   }

   public long maxTimestamp() {
      return this.timestamp();
   }

   public boolean equals(Object other) {
      if(this == other) {
         return true;
      } else if(!(other instanceof Cell)) {
         return false;
      } else {
         Cell that = (Cell)other;
         return this.column().equals(that.column()) && this.isCounterCell() == that.isCounterCell() && this.timestamp() == that.timestamp() && this.ttl() == that.ttl() && this.localDeletionTime() == that.localDeletionTime() && Objects.equals(this.value(), that.value()) && Objects.equals(this.path(), that.path());
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.column(), Boolean.valueOf(this.isCounterCell()), Long.valueOf(this.timestamp()), Integer.valueOf(this.ttl()), Integer.valueOf(this.localDeletionTime()), this.value(), this.path()});
   }

   public String toString() {
      if(this.isCounterCell()) {
         return String.format("[%s=%d ts=%d]", new Object[]{this.column().name, Long.valueOf(CounterContext.instance().total(this.value())), Long.valueOf(this.timestamp())});
      } else {
         AbstractType<?> type = this.column().type;
         return type.isMultiCell()?String.format("[%s%s %s]", new Object[]{this.column().name, type.getString(this.path(), this.value()), this.livenessInfoString()}):(this.isTombstone()?String.format("[%s=<tombstone> %s]", new Object[]{this.column().name, this.livenessInfoString()}):String.format("[%s=%s %s]", new Object[]{this.column().name, type.getString(this.value()), this.livenessInfoString()}));
      }
   }

   private String livenessInfoString() {
      return this.isExpiring()?String.format("ts=%d ttl=%d ldt=%d", new Object[]{Long.valueOf(this.timestamp()), Integer.valueOf(this.ttl()), Integer.valueOf(this.localDeletionTime())}):(this.isTombstone()?String.format("ts=%d ldt=%d", new Object[]{Long.valueOf(this.timestamp()), Integer.valueOf(this.localDeletionTime())}):String.format("ts=%d", new Object[]{Long.valueOf(this.timestamp())}));
   }
}
