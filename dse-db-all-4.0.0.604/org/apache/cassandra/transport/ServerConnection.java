package org.apache.cassandra.transport;

import io.netty.channel.Channel;
import io.reactivex.Single;
import io.reactivex.functions.Function;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.auth.user.UserRolesAndPermissions;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerConnection extends Connection {
   private static final Logger logger = LoggerFactory.getLogger(ServerConnection.class);
   private volatile IAuthenticator.SaslNegotiator saslNegotiator;
   private final ClientState clientState;
   private volatile ServerConnection.State state;
   private AtomicLong inFlightRequests;

   public ServerConnection(Channel channel, ProtocolVersion version, Connection.Tracker tracker) {
      super(channel, version, tracker);
      this.clientState = ClientState.forExternalCalls(channel.remoteAddress(), this);
      this.state = ServerConnection.State.UNINITIALIZED;
      this.inFlightRequests = new AtomicLong(0L);
      channel.attr(Server.ATTR_KEY_CLIENT_STATE).set(this.clientState);
   }

   public Single<QueryState> validateNewMessage(Message.Request request, ProtocolVersion version) {
      Message.Type type = request.type;
      switch (this.state) {
         case UNINITIALIZED: {
            if (type == Message.Type.STARTUP || type == Message.Type.OPTIONS) break;
            throw new ProtocolException(String.format("Unexpected message %s, expecting STARTUP or OPTIONS", new Object[]{type}));
         }
         case AUTHENTICATION: {
            if (type == Message.Type.AUTH_RESPONSE) break;
            throw new ProtocolException(String.format("Unexpected message %s, expecting SASL_RESPONSE", new Object[]{type}));
         }
         case READY: {
            if (type != Message.Type.STARTUP) break;
            throw new ProtocolException("Unexpected message STARTUP, the connection is already initialized");
         }
         default: {
            throw new AssertionError();
         }
      }
      if (this.clientState.getUser() == null) {
         return Single.just(new QueryState(this.clientState, request.getStreamId(), null));
      }
      return DatabaseDescriptor.getAuthManager().getUserRolesAndPermissions(this.clientState.getUser()).map(u -> new QueryState(this.clientState, request.getStreamId(), (UserRolesAndPermissions)u));
   }

   public void applyStateTransition(Message.Type requestType, Message.Type responseType) {
      switch (this.state) {
         case UNINITIALIZED: {
            if (requestType != Message.Type.STARTUP) break;
            if (responseType == Message.Type.AUTHENTICATE) {
               this.state = State.AUTHENTICATION;
               break;
            }
            if (responseType != Message.Type.READY) break;
            this.state = State.READY;
            break;
         }
         case AUTHENTICATION: {
            assert (requestType == Message.Type.AUTH_RESPONSE);
            if (responseType != Message.Type.AUTH_SUCCESS) break;
            this.state = State.READY;
            this.saslNegotiator = null;
            break;
         }
         case READY: {
            break;
         }
         default: {
            throw new AssertionError();
         }
      }
   }

   public IAuthenticator.SaslNegotiator getSaslNegotiator() {
      if(this.saslNegotiator == null) {
         this.saslNegotiator = DatabaseDescriptor.getAuthenticator().newSaslNegotiator(this.getClientAddress());
      }

      return this.saslNegotiator;
   }

   public void onNewRequest() {
      this.inFlightRequests.incrementAndGet();
   }

   public void onRequestCompleted() {
      this.inFlightRequests.decrementAndGet();
   }

   protected InetSocketAddress getRemoteAddress() {
      return this.clientState.isInternal?null:this.clientState.getRemoteAddress();
   }

   protected final InetAddress getClientAddress() {
      InetSocketAddress socketAddress = this.getRemoteAddress();
      return socketAddress == null?null:socketAddress.getAddress();
   }

   public CompletableFuture<Void> waitForInFlightRequests() {
      if(logger.isTraceEnabled()) {
         logger.trace("Waiting for {} in flight requests to complete", Long.valueOf(this.inFlightRequests.get()));
      }

      if(this.inFlightRequests.get() == 0L) {
         return CompletableFuture.completedFuture(null);
      } else {
         CompletableFuture<Void> ret = new CompletableFuture();
         StageManager.getScheduler(Stage.REQUEST_RESPONSE).scheduleDirect(() -> {
            this.checkInFlightRequests(ret);
         }, 1L, TimeUnit.MILLISECONDS);
         return ret;
      }
   }

   private void checkInFlightRequests(CompletableFuture<Void> fut) {
      if(this.inFlightRequests.get() == 0L) {
         fut.complete(null);
      } else {
         StageManager.getScheduler(Stage.REQUEST_RESPONSE).scheduleDirect(() -> {
            this.checkInFlightRequests(fut);
         }, 1L, TimeUnit.MILLISECONDS);
      }

   }

   private static enum State {
      UNINITIALIZED,
      AUTHENTICATION,
      READY;

      private State() {
      }
   }
}
