package org.apache.cassandra.streaming.messages;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.streaming.StreamSession;

public class ReceivedMessage extends StreamMessage {
   public static StreamMessage.Serializer<ReceivedMessage> serializer = new StreamMessage.Serializer<ReceivedMessage>() {
      public ReceivedMessage deserialize(ReadableByteChannel in, StreamMessage.StreamVersion version, StreamSession session) throws IOException {
         DataInputPlus input = new DataInputPlus.DataInputStreamPlus(Channels.newInputStream(in));
         return new ReceivedMessage(TableId.deserialize(input), input.readInt());
      }

      public void serialize(ReceivedMessage message, DataOutputStreamPlus out, StreamMessage.StreamVersion version, StreamSession session) throws IOException {
         message.tableId.serialize(out);
         out.writeInt(message.sequenceNumber);
      }
   };
   public final TableId tableId;
   public final int sequenceNumber;

   public ReceivedMessage(TableId tableId, int sequenceNumber) {
      super(StreamMessage.Type.RECEIVED);
      this.tableId = tableId;
      this.sequenceNumber = sequenceNumber;
   }

   public String toString() {
      StringBuilder sb = new StringBuilder("Received (");
      sb.append(this.tableId).append(", #").append(this.sequenceNumber).append(')');
      return sb.toString();
   }
}
