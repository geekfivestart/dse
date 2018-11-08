package org.apache.cassandra.streaming.messages;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.function.Function;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.UUIDSerializer;
import org.apache.cassandra.utils.versioning.Versioned;

public class StreamInitMessage {
   public static Versioned<StreamMessage.StreamVersion, Serializer<StreamInitMessage>> serializers = StreamMessage.StreamVersion.versioned((v) -> {
      return new Serializer<StreamInitMessage>() {
         public void serialize(StreamInitMessage message, DataOutputPlus out) throws IOException {
            CompactEndpointSerializationHelper.serialize(message.from, out);
            out.writeInt(message.sessionIndex);
            UUIDSerializer.serializer.serialize(message.planId, out);
            out.writeUTF(message.streamOperation.getDescription());
            out.writeBoolean(message.isForOutgoing);
            out.writeBoolean(message.keepSSTableLevel);
            out.writeBoolean(message.pendingRepair != null);
            if(message.pendingRepair != null) {
               UUIDSerializer.serializer.serialize(message.pendingRepair, out);
            }

            out.writeInt(message.previewKind.getSerializationVal());
         }

         public StreamInitMessage deserialize(DataInputPlus in) throws IOException {
            InetAddress from = CompactEndpointSerializationHelper.deserialize(in);
            int sessionIndex = in.readInt();
            UUID planId = UUIDSerializer.serializer.deserialize(in);
            String description = in.readUTF();
            boolean sentByInitiator = in.readBoolean();
            boolean keepSSTableLevel = in.readBoolean();
            UUID pendingRepair = in.readBoolean()?UUIDSerializer.serializer.deserialize(in):null;
            PreviewKind previewKind = PreviewKind.deserialize(in.readInt());
            return new StreamInitMessage(from, sessionIndex, planId, StreamOperation.fromString(description), sentByInitiator, keepSSTableLevel, pendingRepair, previewKind);
         }

         public long serializedSize(StreamInitMessage message) {
            long size = (long)CompactEndpointSerializationHelper.serializedSize(message.from);
            size += (long)TypeSizes.sizeof(message.sessionIndex);
            size += UUIDSerializer.serializer.serializedSize(message.planId);
            size += (long)TypeSizes.sizeof(message.streamOperation.getDescription());
            size += (long)TypeSizes.sizeof(message.isForOutgoing);
            size += (long)TypeSizes.sizeof(message.keepSSTableLevel);
            size += (long)TypeSizes.sizeof(message.pendingRepair != null);
            if(message.pendingRepair != null) {
               size += UUIDSerializer.serializer.serializedSize(message.pendingRepair);
            }

            size += (long)TypeSizes.sizeof(message.previewKind.getSerializationVal());
            return size;
         }
      };
   });
   public final InetAddress from;
   public final int sessionIndex;
   public final UUID planId;
   public final StreamOperation streamOperation;
   public final boolean isForOutgoing;
   public final boolean keepSSTableLevel;
   public final UUID pendingRepair;
   public final PreviewKind previewKind;

   public StreamInitMessage(InetAddress from, int sessionIndex, UUID planId, StreamOperation streamOperation, boolean isForOutgoing, boolean keepSSTableLevel, UUID pendingRepair, PreviewKind previewKind) {
      this.from = from;
      this.sessionIndex = sessionIndex;
      this.planId = planId;
      this.streamOperation = streamOperation;
      this.isForOutgoing = isForOutgoing;
      this.keepSSTableLevel = keepSSTableLevel;
      this.pendingRepair = pendingRepair;
      this.previewKind = previewKind;
   }

   public ByteBuffer createMessage(boolean compress, StreamMessage.StreamVersion version) {
      int header = version.protocolVersion.makeProtocolHeader(compress, true);

      byte[] bytes;
      try {
         int size = Math.toIntExact(((Serializer)serializers.get(version)).serializedSize(this));
         DataOutputBuffer buffer = new DataOutputBufferFixed(size);
         Throwable var7 = null;

         try {
            ((Serializer)serializers.get(version)).serialize(this, buffer);
            bytes = buffer.getData();
         } catch (Throwable var17) {
            var7 = var17;
            throw var17;
         } finally {
            if(buffer != null) {
               if(var7 != null) {
                  try {
                     buffer.close();
                  } catch (Throwable var16) {
                     var7.addSuppressed(var16);
                  }
               } else {
                  buffer.close();
               }
            }

         }
      } catch (IOException var19) {
         throw new RuntimeException(var19);
      }

      assert bytes.length > 0;

      ByteBuffer buffer = ByteBuffer.allocate(8 + bytes.length);
      buffer.putInt(-900387334);
      buffer.putInt(header);
      buffer.put(bytes);
      buffer.flip();
      return buffer;
   }
}
