package org.apache.cassandra.db;

import com.google.common.hash.Hasher;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.HashingUtils;
import org.apache.cassandra.utils.ObjectSizes;

public class DeletionTime implements Comparable<DeletionTime>, IMeasurableMemory {
   private static final long EMPTY_SIZE = ObjectSizes.measure(new DeletionTime(0L, 0));
   public static final DeletionTime LIVE = new DeletionTime(-9223372036854775808L, 2147483647);
   public static final DeletionTime.Serializer serializer = new DeletionTime.Serializer();
   private final long markedForDeleteAt;
   private final int localDeletionTime;

   public DeletionTime(long markedForDeleteAt, int localDeletionTime) {
      this.markedForDeleteAt = markedForDeleteAt;
      this.localDeletionTime = localDeletionTime;
   }

   public long markedForDeleteAt() {
      return this.markedForDeleteAt;
   }

   public int localDeletionTime() {
      return this.localDeletionTime;
   }

   public boolean isLive() {
      return this.markedForDeleteAt() == -9223372036854775808L && this.localDeletionTime() == 2147483647;
   }

   public void digest(Hasher hasher) {
      HashingUtils.updateWithLong(hasher, this.markedForDeleteAt());
   }

   public boolean equals(Object o) {
      if(!(o instanceof DeletionTime)) {
         return false;
      } else {
         DeletionTime that = (DeletionTime)o;
         return this.markedForDeleteAt() == that.markedForDeleteAt() && this.localDeletionTime() == that.localDeletionTime();
      }
   }

   public final int hashCode() {
      return Objects.hash(new Object[]{Long.valueOf(this.markedForDeleteAt()), Integer.valueOf(this.localDeletionTime())});
   }

   public String toString() {
      return this.isLive()?"LIVE":String.format("deletedAt=%d, localDeletion=%d", new Object[]{Long.valueOf(this.markedForDeleteAt()), Integer.valueOf(this.localDeletionTime())});
   }

   public int compareTo(DeletionTime dt) {
      return this.markedForDeleteAt() < dt.markedForDeleteAt()?-1:(this.markedForDeleteAt() > dt.markedForDeleteAt()?1:(this.localDeletionTime() < dt.localDeletionTime()?-1:(this.localDeletionTime() > dt.localDeletionTime()?1:0)));
   }

   public boolean supersedes(DeletionTime dt) {
      return this.markedForDeleteAt() > dt.markedForDeleteAt() || this.markedForDeleteAt() == dt.markedForDeleteAt() && this.localDeletionTime() > dt.localDeletionTime();
   }

   public boolean deletes(LivenessInfo info) {
      return this.deletes(info.timestamp());
   }

   public boolean deletes(Cell cell) {
      return this.deletes(cell.timestamp());
   }

   public boolean deletes(long timestamp) {
      return timestamp <= this.markedForDeleteAt();
   }

   public int dataSize() {
      return 12;
   }

   public long unsharedHeapSize() {
      return EMPTY_SIZE;
   }

   public static class Serializer implements ISerializer<DeletionTime> {
      public Serializer() {
      }

      public void serialize(DeletionTime delTime, DataOutputPlus out) throws IOException {
         out.writeInt(delTime.localDeletionTime());
         out.writeLong(delTime.markedForDeleteAt());
      }

      public DeletionTime deserialize(DataInputPlus in) throws IOException {
         int ldt = in.readInt();
         long mfda = in.readLong();
         return mfda == -9223372036854775808L && ldt == 2147483647?DeletionTime.LIVE:new DeletionTime(mfda, ldt);
      }

      public DeletionTime deserialize(ByteBuffer buf, int offset) {
         int ldt = buf.getInt(offset);
         long mfda = buf.getLong(offset + 4);
         return mfda == -9223372036854775808L && ldt == 2147483647?DeletionTime.LIVE:new DeletionTime(mfda, ldt);
      }

      public void skip(DataInputPlus in) throws IOException {
         in.skipBytesFully(12);
      }

      public long serializedSize(DeletionTime delTime) {
         return (long)(TypeSizes.sizeof(delTime.localDeletionTime()) + TypeSizes.sizeof(delTime.markedForDeleteAt()));
      }
   }
}
