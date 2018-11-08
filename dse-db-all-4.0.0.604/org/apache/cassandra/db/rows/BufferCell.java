package org.apache.cassandra.db.rows;

import java.nio.ByteBuffer;
import org.apache.cassandra.db.ExpirationDateOverflowHandling;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.memory.AbstractAllocator;

public class BufferCell extends AbstractCell {
   private static final long EMPTY_SIZE;
   private final long timestamp;
   private final int ttl;
   private final int localDeletionTime;
   private final ByteBuffer value;
   private final CellPath path;

   public BufferCell(ColumnMetadata column, long timestamp, int ttl, int localDeletionTime, ByteBuffer value, CellPath path) {
      super(column);

      assert !column.isPrimaryKeyColumn();

      assert column.isComplex() == (path != null);

      this.timestamp = timestamp;
      this.ttl = ttl;
      this.localDeletionTime = localDeletionTime;
      this.value = value;
      this.path = path;
   }

   public static BufferCell live(ColumnMetadata column, long timestamp, ByteBuffer value) {
      return live(column, timestamp, value, (CellPath)null);
   }

   public static BufferCell live(ColumnMetadata column, long timestamp, ByteBuffer value, CellPath path) {
      return new BufferCell(column, timestamp, 0, 2147483647, value, path);
   }

   public static BufferCell expiring(ColumnMetadata column, long timestamp, int ttl, int nowInSec, ByteBuffer value) {
      return expiring(column, timestamp, ttl, nowInSec, value, (CellPath)null);
   }

   public static BufferCell expiring(ColumnMetadata column, long timestamp, int ttl, int nowInSec, ByteBuffer value, CellPath path) {
      assert ttl != 0;

      return new BufferCell(column, timestamp, ttl, ExpirationDateOverflowHandling.computeLocalExpirationTime(nowInSec, ttl), value, path);
   }

   public static BufferCell tombstone(ColumnMetadata column, long timestamp, int nowInSec) {
      return tombstone(column, timestamp, nowInSec, (CellPath)null);
   }

   public static BufferCell tombstone(ColumnMetadata column, long timestamp, int nowInSec, CellPath path) {
      return new BufferCell(column, timestamp, 0, nowInSec, ByteBufferUtil.EMPTY_BYTE_BUFFER, path);
   }

   public long timestamp() {
      return this.timestamp;
   }

   public int ttl() {
      return this.ttl;
   }

   public int localDeletionTime() {
      return this.localDeletionTime;
   }

   public ByteBuffer value() {
      return this.value;
   }

   public CellPath path() {
      return this.path;
   }

   public Cell withUpdatedColumn(ColumnMetadata newColumn) {
      return new BufferCell(newColumn, this.timestamp, this.ttl, this.localDeletionTime, this.value, this.path);
   }

   public Cell withUpdatedValue(ByteBuffer newValue) {
      return new BufferCell(this.column, this.timestamp, this.ttl, this.localDeletionTime, newValue, this.path);
   }

   public Cell withUpdatedTimestampAndLocalDeletionTime(long newTimestamp, int newLocalDeletionTime) {
      return new BufferCell(this.column, newTimestamp, this.ttl, newLocalDeletionTime, this.value, this.path);
   }

   public Cell withSkippedValue() {
      return this.withUpdatedValue(ByteBufferUtil.EMPTY_BYTE_BUFFER);
   }

   public Cell copy(AbstractAllocator allocator) {
      return !this.value.hasRemaining()?this:new BufferCell(this.column, this.timestamp, this.ttl, this.localDeletionTime, allocator.clone(this.value), this.path == null?null:this.path.copy(allocator));
   }

   public long unsharedHeapSizeExcludingData() {
      return EMPTY_SIZE + ObjectSizes.sizeOnHeapExcludingData(this.value) + (this.path == null?0L:this.path.unsharedHeapSizeExcludingData());
   }

   static {
      EMPTY_SIZE = ObjectSizes.measure(new BufferCell(ColumnMetadata.regularColumn("", "", "", ByteType.instance), 0L, 0, 0, ByteBufferUtil.EMPTY_BYTE_BUFFER, (CellPath)null));
   }
}
