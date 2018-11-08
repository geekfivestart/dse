package org.apache.cassandra.db.commitlog;

import java.io.IOException;
import java.util.Comparator;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class CommitLogPosition implements Comparable<CommitLogPosition> {
   public static final CommitLogPosition.CommitLogPositionSerializer serializer = new CommitLogPosition.CommitLogPositionSerializer();
   public static final CommitLogPosition NONE = new CommitLogPosition(-1L, 0);
   public final long segmentId;
   public final int position;
   public static final Comparator<CommitLogPosition> comparator = new Comparator<CommitLogPosition>() {
      public int compare(CommitLogPosition o1, CommitLogPosition o2) {
         return o1.segmentId != o2.segmentId?Long.compare(o1.segmentId, o2.segmentId):Integer.compare(o1.position, o2.position);
      }
   };

   public CommitLogPosition(long segmentId, int position) {
      this.segmentId = segmentId;

      assert position >= 0;

      this.position = position;
   }

   public int compareTo(CommitLogPosition other) {
      return comparator.compare(this, other);
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         CommitLogPosition that = (CommitLogPosition)o;
         return this.position != that.position?false:this.segmentId == that.segmentId;
      } else {
         return false;
      }
   }

   public int hashCode() {
      int result = (int)(this.segmentId ^ this.segmentId >>> 32);
      result = 31 * result + this.position;
      return result;
   }

   public String toString() {
      return "CommitLogPosition(segmentId=" + this.segmentId + ", position=" + this.position + ')';
   }

   public CommitLogPosition clone() {
      return new CommitLogPosition(this.segmentId, this.position);
   }

   public static class CommitLogPositionSerializer implements ISerializer<CommitLogPosition> {
      public CommitLogPositionSerializer() {
      }

      public void serialize(CommitLogPosition clsp, DataOutputPlus out) throws IOException {
         out.writeLong(clsp.segmentId);
         out.writeInt(clsp.position);
      }

      public CommitLogPosition deserialize(DataInputPlus in) throws IOException {
         return new CommitLogPosition(in.readLong(), in.readInt());
      }

      public long serializedSize(CommitLogPosition clsp) {
         return (long)(TypeSizes.sizeof(clsp.segmentId) + TypeSizes.sizeof(clsp.position));
      }
   }
}
