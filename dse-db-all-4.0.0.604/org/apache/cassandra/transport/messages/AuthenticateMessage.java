package org.apache.cassandra.transport.messages;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolVersion;

public class AuthenticateMessage extends Message.Response {
   public static final Message.Codec<AuthenticateMessage> codec = new Message.Codec<AuthenticateMessage>() {
      public AuthenticateMessage decode(ByteBuf body, ProtocolVersion version) {
         String authenticator = CBUtil.readString(body);
         return new AuthenticateMessage(authenticator);
      }

      public void encode(AuthenticateMessage msg, ByteBuf dest, ProtocolVersion version) {
         CBUtil.writeString(msg.authenticator, dest);
      }

      public int encodedSize(AuthenticateMessage msg, ProtocolVersion version) {
         return CBUtil.sizeOfString(msg.authenticator);
      }
   };
   public final String authenticator;

   public AuthenticateMessage(String authenticator) {
      super(Message.Type.AUTHENTICATE);
      this.authenticator = authenticator;
   }

   public String toString() {
      return "AUTHENTICATE " + this.authenticator;
   }
}
