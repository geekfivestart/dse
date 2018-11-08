package org.apache.cassandra.transport.messages;

import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolVersion;

public class AuthChallenge extends Message.Response {
   public static final Message.Codec<AuthChallenge> codec = new Message.Codec<AuthChallenge>() {
      public AuthChallenge decode(ByteBuf body, ProtocolVersion version) {
         ByteBuffer b = CBUtil.readValue(body);
         byte[] token = new byte[b.remaining()];
         b.get(token);
         return new AuthChallenge(token);
      }

      public void encode(AuthChallenge challenge, ByteBuf dest, ProtocolVersion version) {
         CBUtil.writeValue(challenge.token, dest);
      }

      public int encodedSize(AuthChallenge challenge, ProtocolVersion version) {
         return CBUtil.sizeOfValue(challenge.token);
      }
   };
   private byte[] token;

   public AuthChallenge(byte[] token) {
      super(Message.Type.AUTH_CHALLENGE);
      this.token = token;
   }

   public byte[] getToken() {
      return this.token;
   }
}
