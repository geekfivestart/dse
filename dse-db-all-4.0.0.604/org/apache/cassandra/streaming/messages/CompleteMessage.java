package org.apache.cassandra.streaming.messages;

import java.nio.channels.ReadableByteChannel;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.streaming.StreamSession;

public class CompleteMessage extends StreamMessage {
   public static StreamMessage.Serializer<CompleteMessage> serializer = new StreamMessage.Serializer<CompleteMessage>() {
      public CompleteMessage deserialize(ReadableByteChannel in, StreamMessage.StreamVersion version, StreamSession session) {
         return new CompleteMessage();
      }

      public void serialize(CompleteMessage message, DataOutputStreamPlus out, StreamMessage.StreamVersion version, StreamSession session) {
      }
   };

   public CompleteMessage() {
      super(StreamMessage.Type.COMPLETE);
   }

   public String toString() {
      return "Complete";
   }
}
