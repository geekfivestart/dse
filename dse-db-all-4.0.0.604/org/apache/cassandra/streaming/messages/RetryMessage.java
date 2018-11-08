package org.apache.cassandra.streaming.messages;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.UUID;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.utils.UUIDSerializer;

/** @deprecated */
@Deprecated
public class RetryMessage extends StreamMessage {
   public static StreamMessage.Serializer<RetryMessage> serializer = new StreamMessage.Serializer<RetryMessage>() {
      public RetryMessage deserialize(ReadableByteChannel in, StreamMessage.StreamVersion version, StreamSession session) throws IOException {
         DataInputPlus input = new DataInputPlus.DataInputStreamPlus(Channels.newInputStream(in));
         return new RetryMessage(UUIDSerializer.serializer.deserialize(input), input.readInt());
      }

      public void serialize(RetryMessage message, DataOutputStreamPlus out, StreamMessage.StreamVersion version, StreamSession session) throws IOException {
         UUIDSerializer.serializer.serialize((UUID)message.cfId, out);
         out.writeInt(message.sequenceNumber);
      }
   };
   public final UUID cfId;
   public final int sequenceNumber;

   public RetryMessage(UUID cfId, int sequenceNumber) {
      super(StreamMessage.Type.RETRY);
      this.cfId = cfId;
      this.sequenceNumber = sequenceNumber;
   }

   public String toString() {
      StringBuilder sb = new StringBuilder("Retry (");
      sb.append(this.cfId).append(", #").append(this.sequenceNumber).append(')');
      return sb.toString();
   }
}
