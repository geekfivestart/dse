package org.apache.cassandra.streaming;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;
import java.util.function.Function;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.streaming.messages.StreamMessage;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;

public class StreamSummary implements Serializable {
   public static final Versioned<StreamMessage.StreamVersion, Serializer<StreamSummary>> serializers = StreamMessage.StreamVersion.versioned(StreamSummary.StreamSummarySerializer::<init>);
   public final TableId tableId;
   public final int files;
   public final long totalSize;

   public StreamSummary(TableId tableId, int files, long totalSize) {
      this.tableId = tableId;
      this.files = files;
      this.totalSize = totalSize;
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         StreamSummary summary = (StreamSummary)o;
         return this.files == summary.files && this.totalSize == summary.totalSize && this.tableId.equals(summary.tableId);
      } else {
         return false;
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.tableId, Integer.valueOf(this.files), Long.valueOf(this.totalSize)});
   }

   public String toString() {
      StringBuilder sb = new StringBuilder("StreamSummary{");
      sb.append("path=").append(this.tableId);
      sb.append(", files=").append(this.files);
      sb.append(", totalSize=").append(this.totalSize);
      sb.append('}');
      return sb.toString();
   }

   public static class StreamSummarySerializer extends VersionDependent<StreamMessage.StreamVersion> implements Serializer<StreamSummary> {
      protected StreamSummarySerializer(StreamMessage.StreamVersion version) {
         super(version);
      }

      public void serialize(StreamSummary summary, DataOutputPlus out) throws IOException {
         summary.tableId.serialize(out);
         out.writeInt(summary.files);
         out.writeLong(summary.totalSize);
      }

      public StreamSummary deserialize(DataInputPlus in) throws IOException {
         TableId tableId = TableId.deserialize(in);
         int files = in.readInt();
         long totalSize = in.readLong();
         return new StreamSummary(tableId, files, totalSize);
      }

      public long serializedSize(StreamSummary summary) {
         long size = (long)summary.tableId.serializedSize();
         size += (long)TypeSizes.sizeof(summary.files);
         size += (long)TypeSizes.sizeof(summary.totalSize);
         return size;
      }
   }
}
