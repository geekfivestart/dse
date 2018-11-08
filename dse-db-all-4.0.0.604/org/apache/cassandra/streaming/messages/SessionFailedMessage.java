package org.apache.cassandra.streaming.messages;

import java.nio.channels.ReadableByteChannel;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.streaming.StreamSession;

public class SessionFailedMessage extends StreamMessage {
   public static StreamMessage.Serializer<SessionFailedMessage> serializer = new StreamMessage.Serializer<SessionFailedMessage>() {
      public SessionFailedMessage deserialize(ReadableByteChannel in, StreamMessage.StreamVersion version, StreamSession session) {
         return new SessionFailedMessage();
      }

      public void serialize(SessionFailedMessage message, DataOutputStreamPlus out, StreamMessage.StreamVersion version, StreamSession session) {
      }
   };

   public SessionFailedMessage() {
      super(StreamMessage.Type.SESSION_FAILED);
   }

   public String toString() {
      return "Session Failed";
   }
}
