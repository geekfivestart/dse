package org.apache.cassandra.db.partitions;

import io.reactivex.functions.Action;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Function;
import org.apache.cassandra.db.EncodingVersion;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.UnfilteredPartitionSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.flow.FlowSource;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;

public class UnfilteredPartitionsSerializer {
   private static final Versioned<EncodingVersion, UnfilteredPartitionsSerializer.Serializer> serializers = EncodingVersion.versioned((x$0) -> {
      return new UnfilteredPartitionsSerializer.Serializer(x$0);
   });

   public UnfilteredPartitionsSerializer() {
   }

   public static UnfilteredPartitionsSerializer.Serializer serializerForIntraNode(EncodingVersion version) {
      return (UnfilteredPartitionsSerializer.Serializer)serializers.get(version);
   }

   public static class Serializer extends VersionDependent<EncodingVersion> {
      private Serializer(EncodingVersion version) {
         super(version);
      }

      public Flow<ByteBuffer> serialize(Flow<FlowableUnfilteredPartition> partitions, ColumnFilter selection) {
         DataOutputBuffer out = new DataOutputBuffer();

         try {
            out.writeBoolean(false);
         } catch (IOException var5) {
            throw new AssertionError(var5);
         }

         return partitions.flatProcess((partition) -> {
            out.writeBoolean(true);
            return ((UnfilteredPartitionSerializer)UnfilteredPartitionSerializer.serializers.get(this.version)).serialize((FlowableUnfilteredPartition)partition, (ColumnFilter)selection, out);
         }).map((VOID) -> {
            out.writeBoolean(false);
            return out.trimmedBuffer();
         });
      }

      public Flow<FlowableUnfilteredPartition> deserialize(ByteBuffer buffer, TableMetadata metadata, ColumnFilter selection, SerializationHelper.Flag flag) {
         return new UnfilteredPartitionsSerializer.Serializer.DeserializePartitionsFlow(buffer, metadata, selection, flag);
      }

      private class DeserializePartitionsFlow extends FlowSource<FlowableUnfilteredPartition> {
         private final DataInputBuffer in;
         private final TableMetadata metadata;
         private final ColumnFilter selection;
         private final SerializationHelper.Flag flag;
         private volatile FlowableUnfilteredPartition current;

         private DeserializePartitionsFlow(ByteBuffer buffer, TableMetadata metadata, ColumnFilter selection, SerializationHelper.Flag flag) {
            ByteBuffer buf = buffer.duplicate();
            buf.get();
            this.in = new DataInputBuffer(buf, false);
            this.metadata = metadata;
            this.selection = selection;
            this.flag = flag;
         }

         public void requestNext() {
            try {
               if(this.current != null) {
                  throw new IllegalStateException("Previous partition was not closed!");
               }

               boolean hasNext = this.in.readBoolean();
               if(hasNext) {
                  UnfilteredPartitionSerializer serializer = (UnfilteredPartitionSerializer)UnfilteredPartitionSerializer.serializers.get(Serializer.this.version);
                  FlowableUnfilteredPartition fup = serializer.deserializeToFlow(this.in, this.metadata, (ColumnFilter)this.selection, (SerializationHelper.Flag)this.flag);
                  this.current = fup.withContent(fup.content().doOnClose(() -> {
                     this.current = null;
                  }));
                  this.subscriber.onNext(this.current);
               } else {
                  this.subscriber.onComplete();
               }
            } catch (Throwable var4) {
               this.subscriber.onError(var4);
            }

         }

         public void close() throws Exception {
            this.in.close();
         }

         public String toString() {
            return Flow.formatTrace("deserialize-partitions", (Object)this.subscriber);
         }
      }
   }
}
