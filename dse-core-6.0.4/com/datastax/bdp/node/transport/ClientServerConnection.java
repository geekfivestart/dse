package com.datastax.bdp.node.transport;

import com.datastax.bdp.node.transport.internal.FailedProcessorException;
import com.datastax.bdp.node.transport.internal.Handshake;
import com.datastax.bdp.node.transport.internal.SystemMessageTypes;
import com.datastax.bdp.node.transport.internal.UnsupportedMessageException;
import com.datastax.bdp.server.DseDaemon;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.local.LocalChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Exchanger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientServerConnection implements Comparable<ClientServerConnection> {
   private static final Logger logger = LoggerFactory.getLogger(ClientServerConnection.class);
   private final ConcurrentMap<Long, ClientContext> pendingRequests = new ConcurrentHashMap();
   private final AtomicInteger refs = new AtomicInteger();
   private final AtomicBoolean shutdown = new AtomicBoolean(false);
   private final Channel channel;
   private final int handshakeTimeoutSecs;
   private volatile long timestamp;
   private volatile boolean error;
   private volatile byte version = -128;

   public ClientServerConnection(Channel channel, int handshakeTimeoutSecs) {
      this.channel = channel;
      this.handshakeTimeoutSecs = handshakeTimeoutSecs;
      this.timestamp = System.currentTimeMillis();
   }

   public void configure(MessageCodec codec) {
      this.maybeWaitForSslHandshake();
      ChannelPipeline pipeline = this.channel.pipeline();
      pipeline.addLast(new ChannelHandler[]{new ClientServerConnection.ChannelStateHandler(null)});
      pipeline.addLast(codec.newPipeline());
      pipeline.addLast(new ChannelHandler[]{new ClientServerConnection.MessageResponseHandler(null)});
      if(!(this.channel instanceof LocalChannel)) {
         this.doVersionHandshake(codec.getCurrentVersion());
      } else {
         this.version = codec.getCurrentVersion();
      }

   }

   public long getTimestamp() {
      return this.timestamp;
   }

   public boolean isOlderThan(long time, TimeUnit unit) {
      return System.currentTimeMillis() - TimeUnit.MILLISECONDS.convert(time, unit) > this.timestamp;
   }

   public boolean isFaulty() {
      return this.error || this.shutdown.get();
   }

   public boolean isActive() {
      return !this.pendingRequests.isEmpty();
   }

   public boolean isAcquired() {
      return this.refs.get() > 0;
   }

   public int getAcquired() {
      return this.refs.get();
   }

   public int acquire() {
      this.timestamp = System.currentTimeMillis();
      return this.refs.incrementAndGet();
   }

   public void send(final ClientContext context, Message message) {
      this.timestamp = System.currentTimeMillis();
      if(!this.error) {
         this.pendingRequests.put(Long.valueOf(context.id), context);

         try {
            message.trySetVersion(this.version);
            logger.debug("Sending request: {}", Long.valueOf(context.id));
            this.channel.writeAndFlush(message).addListener(new GenericFutureListener<Future<Void>>() {
               public void operationComplete(Future<Void> f) throws Exception {
                  ClientServerConnection.logger.trace("Sending request {} completed; success={}", Long.valueOf(context.id), Boolean.valueOf(f.isSuccess()));
                  if(!f.isSuccess()) {
                     ClientServerConnection.this.logConnectionError("Error sending request: " + context.id, f.cause());
                     ClientServerConnection.this.error = true;
                     ClientServerConnection.this.pendingRequests.remove(Long.valueOf(context.id));
                     ClientServerConnection.this.refs.decrementAndGet();
                     context.onError(ClientServerConnection.this.channel, f.cause());
                  }

               }
            });
         } catch (Throwable var4) {
            this.logConnectionError("Error sending request: " + context.id, var4);
            this.error = true;
            this.pendingRequests.remove(Long.valueOf(context.id));
            this.refs.decrementAndGet();
            context.onError(this.channel, var4);
         }
      } else {
         IllegalStateException cause = new IllegalStateException("Cannot send on a faulty channel! Request was: " + message);
         context.onError(this.channel, cause);
      }

   }

   public boolean shutdown() {
      if(!this.shutdown.compareAndSet(false, true)) {
         return false;
      } else {
         logger.debug("Shutdown channel: {}", this.channel.localAddress());
         if(!this.channel.eventLoop().isShuttingDown() && !this.channel.eventLoop().isShutdown() && !this.channel.eventLoop().isTerminated()) {
            this.channel.close().syncUninterruptibly();
         } else {
            logger.debug("Not closing channel because the associated event loop is either shutting down or has shut down");
         }

         return true;
      }
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         ClientServerConnection that = (ClientServerConnection)o;
         return this.channel.equals(that.channel);
      } else {
         return false;
      }
   }

   public int hashCode() {
      return this.channel.hashCode();
   }

   public int compareTo(ClientServerConnection other) {
      return this.channel.compareTo(other.channel);
   }

   private void maybeWaitForSslHandshake() {
      SslHandler sslHandler = (SslHandler)this.channel.pipeline().get(SslHandler.class);
      if(sslHandler != null) {
         sslHandler.handshakeFuture().syncUninterruptibly();
      }

   }

   private void doVersionHandshake(byte currentVersion) {
      long start = System.currentTimeMillis();
      long timeout = TimeUnit.MILLISECONDS.convert((long)this.handshakeTimeoutSecs, TimeUnit.SECONDS);
      Handshake handshake = null;

      do {
         Exchanger<Handshake> exchange = new Exchanger();
         ClientServerConnection.HandshakeContext context = new ClientServerConnection.HandshakeContext(Thread.currentThread(), exchange, timeout);
         Message message = new Message(context.id, SystemMessageTypes.HANDSHAKE, new Handshake(currentVersion));
         message.trySetVersion(-1);
         this.send(context, message);

         try {
            handshake = (Handshake)exchange.exchange((Object)null, timeout, TimeUnit.MILLISECONDS);
         } catch (InterruptedException var12) {
            if(context.hasError) {
               if(context.error != null) {
                  throw new RuntimeException(context.error);
               }
               break;
            }
         } catch (TimeoutException var13) {
            break;
         }

         long now = System.currentTimeMillis();
         timeout -= now - start;
         start = now;
      } while(handshake == null && timeout > 0L);

      if(handshake == null) {
         throw new RuntimeException(String.format("Failed handshake due to exhausted %s seconds timeout on channel %s.", new Object[]{Integer.valueOf(this.handshakeTimeoutSecs), this.channel}));
      } else {
         this.version = handshake.version;
      }
   }

   private void terminatePendingRequests() {
      logger.info("Terminating pending requests towards: {}", this.channel.remoteAddress());
      Iterator var1 = this.pendingRequests.entrySet().iterator();

      while(var1.hasNext()) {
         Entry<Long, ClientContext> entry = (Entry)var1.next();
         ClientContext context = (ClientContext)this.pendingRequests.remove(entry.getKey());
         if(context != null) {
            try {
               context.onError(this.channel, new IOException("The channel has been terminated."));
            } finally {
               this.refs.decrementAndGet();
            }
         }
      }

   }

   private void logConnectionError(String message, Throwable ex) {
      if(DseDaemon.isStopped()) {
         logger.debug(message, ex);
      } else {
         logger.warn(message, ex);
      }

   }

   private class MessageResponseHandler extends ChannelInboundHandlerAdapter {
      private MessageResponseHandler() {
      }

      public void channelRead(ChannelHandlerContext channelHandlerContext, Object input) throws Exception {
         ClientServerConnection.this.timestamp = System.currentTimeMillis();
         if(input instanceof Message) {
            Message message = (Message)input;
            ClientServerConnection.logger.trace("Read response for request {} ", Long.valueOf(message.getId()));
            ClientContext context;
            if(message.getFlags().contains(Message.Flag.UNSUPPORTED_MESSAGE)) {
               context = (ClientContext)ClientServerConnection.this.pendingRequests.remove(Long.valueOf(message.getId()));
               if(context == null) {
                  throw (UnsupportedMessageException)message.getBody();
               }

               try {
                  context.onError(ClientServerConnection.this.channel, (UnsupportedMessageException)message.getBody());
               } finally {
                  ClientServerConnection.this.refs.decrementAndGet();
               }
            } else if(message.getFlags().contains(Message.Flag.FAILED_PROCESSOR)) {
               context = (ClientContext)ClientServerConnection.this.pendingRequests.remove(Long.valueOf(message.getId()));
               if(context == null) {
                  throw (FailedProcessorException)message.getBody();
               }

               try {
                  FailedProcessorException exception = (FailedProcessorException)message.getBody();
                  context.onError(ClientServerConnection.this.channel, exception.fillInStackTrace());
               } finally {
                  ClientServerConnection.this.refs.decrementAndGet();
               }
            } else {
               context = (ClientContext)ClientServerConnection.this.pendingRequests.remove(Long.valueOf(message.getId()));
               if(context != null) {
                  try {
                     context.onResponse(message.getBody());
                  } finally {
                     ClientServerConnection.this.refs.decrementAndGet();
                  }
               } else {
                  ClientServerConnection.logger.warn("There was no pending request for id {}. It was most likely terminated on endpoint failure.", Long.valueOf(message.getId()));
               }
            }
         } else {
            ClientServerConnection.logger.warn("Read message of unknown type: {}", input.getClass().getCanonicalName());
         }

      }

      public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
         ClientServerConnection.this.logConnectionError(cause.getMessage(), cause);
      }
   }

   private class ChannelStateHandler extends ChannelDuplexHandler {
      private ChannelStateHandler() {
      }

      public void channelInactive(ChannelHandlerContext ctx) throws Exception {
         ClientServerConnection.this.error = true;
         ClientServerConnection.this.terminatePendingRequests();
      }
   }

   private class HandshakeContext extends ClientContext<Handshake> {
      private final Thread handshaker;
      private final Exchanger<Handshake> exchanger;
      private final long timeout;
      public volatile Throwable error;
      public volatile boolean hasError;

      public HandshakeContext(Thread var1, Exchanger<Handshake> handshaker, long exchanger) {
         this.handshaker = handshaker;
         this.exchanger = exchanger;
         this.timeout = timeout;
      }

      public void onError(Channel channel, Throwable e) {
         if(e != null) {
            ClientServerConnection.logger.warn(e.getMessage(), e);
         }

         this.error = e;
         this.hasError = true;
         this.handshaker.interrupt();
      }

      public void onResponse(Handshake response) {
         try {
            this.exchanger.exchange(response, this.timeout, TimeUnit.MILLISECONDS);
         } catch (InterruptedException var3) {
            this.handshaker.interrupt();
         } catch (TimeoutException var4) {
            ;
         }

      }
   }
}
