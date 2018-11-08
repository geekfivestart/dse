package org.apache.cassandra.transport.messages;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolVersion;

public class EventMessage extends Message.Response {
   public static final Message.Codec<EventMessage> codec = new Message.Codec<EventMessage>() {
      public EventMessage decode(ByteBuf body, ProtocolVersion version) {
         return new EventMessage(Event.deserialize(body, version));
      }

      public void encode(EventMessage msg, ByteBuf dest, ProtocolVersion version) {
         msg.event.serialize(dest, version);
      }

      public int encodedSize(EventMessage msg, ProtocolVersion version) {
         return msg.event.serializedSize(version);
      }
   };
   public final Event event;

   public EventMessage(Event event) {
      super(Message.Type.EVENT);
      this.event = event;
      this.setStreamId(-1);
   }

   public String toString() {
      return "EVENT " + this.event;
   }
}
