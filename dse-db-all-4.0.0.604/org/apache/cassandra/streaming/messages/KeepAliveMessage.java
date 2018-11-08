package org.apache.cassandra.streaming.messages;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.streaming.StreamSession;

public class KeepAliveMessage extends StreamMessage {
   public static StreamMessage.Serializer<KeepAliveMessage> serializer = new StreamMessage.Serializer<KeepAliveMessage>() {
      public KeepAliveMessage deserialize(ReadableByteChannel in, StreamMessage.StreamVersion version, StreamSession session) throws IOException {
         return new KeepAliveMessage();
      }

      public void serialize(KeepAliveMessage message, DataOutputStreamPlus out, StreamMessage.StreamVersion version, StreamSession session) {
      }
   };

   public KeepAliveMessage() {
      super(StreamMessage.Type.KEEP_ALIVE);
   }

   public String toString() {
      return "keep-alive";
   }
}
