package org.apache.cassandra.streaming.compress;

import java.io.IOException;
import java.util.function.Function;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.streaming.messages.StreamMessage;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.versioning.Versioned;

public class CompressionInfo {
   public static final Versioned<StreamMessage.StreamVersion, Serializer<CompressionInfo>> serializers = StreamMessage.StreamVersion.versioned((v) -> {
      return new Serializer<CompressionInfo>() {
         public void serialize(CompressionInfo info, DataOutputPlus out) throws IOException {
            if(info == null) {
               out.writeInt(-1);
            } else {
               int chunkCount = info.chunks.length;
               out.writeInt(chunkCount);

               for(int i = 0; i < chunkCount; ++i) {
                  CompressionMetadata.Chunk.serializer.serialize(info.chunks[i], out);
               }

               ((Serializer)CompressionParams.serializers.get(v)).serialize(info.parameters, out);
            }
         }

         public CompressionInfo deserialize(DataInputPlus in) throws IOException {
            int chunkCount = in.readInt();
            if(chunkCount < 0) {
               return null;
            } else {
               CompressionMetadata.Chunk[] chunks = new CompressionMetadata.Chunk[chunkCount];

               for(int i = 0; i < chunkCount; ++i) {
                  chunks[i] = (CompressionMetadata.Chunk)CompressionMetadata.Chunk.serializer.deserialize(in);
               }

               CompressionParams parameters = (CompressionParams)((Serializer)CompressionParams.serializers.get(v)).deserialize(in);
               return new CompressionInfo(chunks, parameters);
            }
         }

         public long serializedSize(CompressionInfo info) {
            if(info == null) {
               return (long)TypeSizes.sizeof((int)-1);
            } else {
               int chunkCount = info.chunks.length;
               long size = (long)TypeSizes.sizeof(chunkCount);

               for(int i = 0; i < chunkCount; ++i) {
                  size += CompressionMetadata.Chunk.serializer.serializedSize(info.chunks[i]);
               }

               size += ((Serializer)CompressionParams.serializers.get(v)).serializedSize(info.parameters);
               return size;
            }
         }
      };
   });
   public final CompressionMetadata.Chunk[] chunks;
   public final CompressionParams parameters;

   public CompressionInfo(CompressionMetadata.Chunk[] chunks, CompressionParams parameters) {
      assert chunks != null && parameters != null;

      this.chunks = chunks;
      this.parameters = parameters;
   }
}
