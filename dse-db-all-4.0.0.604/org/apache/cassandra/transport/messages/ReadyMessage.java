package org.apache.cassandra.transport.messages;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolVersion;

public class ReadyMessage extends Message.Response {
   public static final Message.Codec<ReadyMessage> codec = new Message.Codec<ReadyMessage>() {
      public ReadyMessage decode(ByteBuf body, ProtocolVersion version) {
         return new ReadyMessage();
      }

      public void encode(ReadyMessage msg, ByteBuf dest, ProtocolVersion version) {
      }

      public int encodedSize(ReadyMessage msg, ProtocolVersion version) {
         return 0;
      }
   };

   public ReadyMessage() {
      super(Message.Type.READY);
   }

   public String toString() {
      return "READY";
   }
}
