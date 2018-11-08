package org.apache.cassandra.transport.messages;

import io.netty.buffer.ByteBuf;
import io.reactivex.Single;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.metrics.AuthMetrics;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.ServerConnection;
import org.apache.cassandra.utils.flow.RxThreads;

public class AuthResponse extends Message.Request {
   public static final Message.Codec<AuthResponse> codec = new Message.Codec<AuthResponse>() {
      public AuthResponse decode(ByteBuf body, ProtocolVersion version) {
         if(version == ProtocolVersion.V1) {
            throw new ProtocolException("SASL Authentication is not supported in version 1 of the protocol");
         } else {
            ByteBuffer b = CBUtil.readValue(body);
            byte[] token = new byte[b.remaining()];
            b.get(token);
            return new AuthResponse(token);
         }
      }

      public void encode(AuthResponse response, ByteBuf dest, ProtocolVersion version) {
         CBUtil.writeValue(response.token, dest);
      }

      public int encodedSize(AuthResponse response, ProtocolVersion version) {
         return CBUtil.sizeOfValue(response.token);
      }
   };
   private final byte[] token;

   public AuthResponse(byte[] token) {
      super(Message.Type.AUTH_RESPONSE);

      assert token != null;

      this.token = token;
   }

   public Single<? extends Message.Response> execute(Single<QueryState> queryState, long queryStartNanoTime) {
      return RxThreads.subscribeOnIo(Single.fromCallable(() -> {
         try {
            IAuthenticator.SaslNegotiator negotiator = ((ServerConnection)this.connection).getSaslNegotiator();
            byte[] challenge = negotiator.evaluateResponse(this.token);
            if(negotiator.isComplete()) {
               AuthenticatedUser user = negotiator.getAuthenticatedUser();
               ((QueryState)queryState.blockingGet()).getClientState().login(user).blockingGet();
               AuthMetrics.instance.markSuccess();
               return new AuthSuccess(challenge);
            } else {
               return new AuthChallenge(challenge);
            }
         } catch (AuthenticationException var5) {
            AuthMetrics.instance.markFailure();
            return ErrorMessage.fromException(var5);
         }
      }), TPCTaskType.AUTHENTICATION);
   }
}
