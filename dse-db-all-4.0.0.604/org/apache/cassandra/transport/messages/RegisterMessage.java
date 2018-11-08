package org.apache.cassandra.transport.messages;

import io.netty.buffer.ByteBuf;
import io.reactivex.Single;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Connection;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.transport.ServerConnection;

public class RegisterMessage extends Message.Request {
   public static final Message.Codec<RegisterMessage> codec = new Message.Codec<RegisterMessage>() {
      public RegisterMessage decode(ByteBuf body, ProtocolVersion version) {
         int length = body.readUnsignedShort();
         List<Event.Type> eventTypes = new ArrayList(length);

         for(int i = 0; i < length; ++i) {
            eventTypes.add(CBUtil.readEnumValue(Event.Type.class, body));
         }

         return new RegisterMessage(eventTypes);
      }

      public void encode(RegisterMessage msg, ByteBuf dest, ProtocolVersion version) {
         dest.writeShort(msg.eventTypes.size());
         Iterator var4 = msg.eventTypes.iterator();

         while(var4.hasNext()) {
            Event.Type type = (Event.Type)var4.next();
            CBUtil.writeEnumValue(type, dest);
         }

      }

      public int encodedSize(RegisterMessage msg, ProtocolVersion version) {
         int size = 2;

         Event.Type type;
         for(Iterator var4 = msg.eventTypes.iterator(); var4.hasNext(); size += CBUtil.sizeOfEnumValue(type)) {
            type = (Event.Type)var4.next();
         }

         return size;
      }
   };
   public final List<Event.Type> eventTypes;

   public RegisterMessage(List<Event.Type> eventTypes) {
      super(Message.Type.REGISTER);
      this.eventTypes = eventTypes;
   }

   public Single<? extends Message.Response> execute(Single<QueryState> state, long queryStartNanoTime) {
      assert this.connection instanceof ServerConnection;

      Connection.Tracker tracker = this.connection.getTracker();

      assert tracker instanceof Server.ConnectionTracker;

      Iterator var5 = this.eventTypes.iterator();

      while(var5.hasNext()) {
         Event.Type type = (Event.Type)var5.next();
         if(type.minimumVersion.isGreaterThan(this.connection.getVersion())) {
            throw new ProtocolException("Event " + type.name() + " not valid for protocol version " + this.connection.getVersion());
         }

         ((Server.ConnectionTracker)tracker).register(type, this.connection().channel());
      }

      return Single.just(new ReadyMessage());
   }

   public String toString() {
      return "REGISTER " + this.eventTypes;
   }
}
