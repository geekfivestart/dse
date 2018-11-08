package com.datastax.bdp.node.transport.internode;

import com.datastax.bdp.node.transport.ClientContext;
import com.datastax.bdp.node.transport.Message;
import com.datastax.bdp.node.transport.MessageClient;
import com.datastax.bdp.node.transport.MessageType;
import com.datastax.bdp.util.Addresses;
import io.netty.channel.Channel;
import java.io.IOException;
import java.net.InetAddress;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

public class InternodeClient {
   private final Supplier<MessageClient> remoteClient;
   private final Supplier<MessageClient> localClient;
   private final String localName;
   private final int port;

   public InternodeClient(Supplier<MessageClient> remoteClient, Supplier<MessageClient> localClient, String localName, int port) {
      this.remoteClient = remoteClient;
      this.localClient = localClient;
      this.localName = localName;
      this.port = port;
   }

   public final <I, O> void sendAsync(InetAddress address, ClientContext<O> context, Message<I> rawMessage) {
      if(Addresses.Internode.isLocalEndpoint(address)) {
         ((MessageClient)this.localClient.get()).sendTo(this.localName, this.port, context, rawMessage);
      } else {
         ((MessageClient)this.remoteClient.get()).sendTo(Addresses.Internode.getPreferredHost(address).getHostAddress(), this.port, context, rawMessage);
      }

   }

   public final <I, O> Future<O> sendAsync(InetAddress address, Message<I> message) {
      InternodeClient.FutureContext<O> ctx = new InternodeClient.FutureContext();
      this.sendAsync(address, (ClientContext)ctx, (Message)message);
      return ctx.future;
   }

   public final <I, O> Future<O> sendAsync(InetAddress address, MessageType type, I body) {
      InternodeClient.FutureContext<O> ctx = new InternodeClient.FutureContext();
      this.sendAsync(address, (ClientContext)ctx, (Message)(new Message(ctx.id, type, body)));
      return ctx.future;
   }

   public <I, O> O sendSync(InetAddress address, Message<I> message, Duration timeout) throws IOException {
      Future<O> future = this.sendAsync(address, message);
      return this.get(future, timeout);
   }

   public <I, O> O sendSync(InetAddress address, MessageType msgType, I message, Duration timeout) throws IOException {
      Future<O> future = this.sendAsync(address, msgType, message);
      return this.get(future, timeout);
   }

   private <O> O get(Future<O> future, Duration timeout) throws IOException {
      try {
         return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException var4) {
         Thread.currentThread().interrupt();
         throw new IOException("Interrupted.", var4);
      } catch (ExecutionException var5) {
         throw new IOException("Execution failed.", var5.getCause());
      } catch (TimeoutException var6) {
         throw new IllegalStateException("No result received within timeout: " + timeout, var6);
      }
   }

   private static class FutureContext<Response> extends ClientContext<Response> {
      private final CompletableFuture<Response> future;

      private FutureContext() {
         this.future = new CompletableFuture();
      }

      public void onResponse(Response response) {
         this.future.complete(response);
      }

      public void onError(Channel channel, Throwable e) {
         this.future.completeExceptionally(e);
      }
   }
}
