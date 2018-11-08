package com.datastax.bdp.node.transport;

import com.datastax.bdp.node.transport.internal.FailedProcessorException;
import com.datastax.bdp.node.transport.internal.HandshakeProcessor;
import com.datastax.bdp.node.transport.internal.SystemMessageTypes;
import com.datastax.bdp.node.transport.internal.UnsupportedMessageException;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.GlobalEventExecutor;
import java.net.InetAddress;
import java.nio.channels.ClosedChannelException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MessageServer {
   protected final ChannelGroup channels;
   protected final List<EventExecutorGroup> groups;
   protected final int acceptorThreads;
   protected final int workerThreads;
   protected final Optional<SSLOptions> sslOptions;
   protected final String name;
   private final Logger logger;
   private final Map<MessageType, ServerProcessor> processors;
   private final ExecutorService workExecutor;
   private final MessageCodec codec;

   public static MessageServer.Builder newBuilder() {
      return new MessageServer.Builder(null);
   }

   public MessageServer(int acceptorThreads, int workerThreads, Optional<SSLOptions> sslOptions, Map<MessageType, ServerProcessor> processors, MessageCodec codec, String name) {
      this.channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
      this.groups = new CopyOnWriteArrayList();
      this.logger = LoggerFactory.getLogger(this.getClass());
      this.acceptorThreads = acceptorThreads;
      this.workerThreads = workerThreads;
      this.sslOptions = sslOptions;
      this.processors = new ConcurrentHashMap(processors);
      this.codec = codec;
      this.name = name;
      this.workExecutor = Executors.newFixedThreadPool(Math.max(1, workerThreads / 4), (new ThreadFactoryBuilder()).setDaemon(true).setNameFormat(this.getClass().getSimpleName() + " query worker - %s").build());
   }

   public void bind() throws InterruptedException {
      ServerBootstrap bootstrap = this.bootstrap();
      bootstrap.childOption(ChannelOption.ALLOCATOR, CBUtil.allocator);
      bootstrap.childHandler(new ChannelInitializer() {
         protected void initChannel(Channel c) throws Exception {
            ChannelPipeline pipeline = c.pipeline();
            MessageServer.this.sslOptions.ifPresent((sslOptions) -> {
               pipeline.addFirst(new ChannelHandler[]{sslOptions.createServerSslHandler()});
            });
            pipeline.addLast(new ChannelHandler[]{MessageServer.this.new ChannelStateHandler(null)});
            pipeline.addLast(MessageServer.this.codec.newPipeline());
            pipeline.addLast(new ChannelHandler[]{MessageServer.this.new MessageRequestHandler(null)});
         }
      });
      Iterator var2 = this.doBind(bootstrap).iterator();

      while(var2.hasNext()) {
         ChannelFuture channelFuture = (ChannelFuture)var2.next();
         this.channels.add(channelFuture.sync().channel());
      }

      this.processors.put(SystemMessageTypes.HANDSHAKE, new HandshakeProcessor(this.codec.getCurrentVersion()));
   }

   public void shutdown() {
      this.channels.close().awaitUninterruptibly();
      Iterator var1 = this.groups.iterator();

      while(var1.hasNext()) {
         EventExecutorGroup group = (EventExecutorGroup)var1.next();
         group.shutdownGracefully().syncUninterruptibly();
      }

      this.workExecutor.shutdown();
      this.logger.info("{} message server finished shutting down.", this.name);
   }

   protected abstract ServerBootstrap bootstrap();

   protected abstract Collection<ChannelFuture> doBind(ServerBootstrap var1);

   private class MessageRequestHandler extends ChannelInboundHandlerAdapter {
      private MessageRequestHandler() {
      }

      public void channelRead(ChannelHandlerContext context, Object input) throws Exception {
         if(input instanceof Message) {
            MessageServer.this.logger.trace("Received request: {}", Long.valueOf(((Message)input).getId()));
            MessageServer.this.workExecutor.execute(() -> {
               ServerProcessor processor = null;
               Message request = (Message)input;
               MessageServer.this.logger.trace("Handling request message {}", Long.valueOf(request.getId()));
               Message response;
               if(request.getFlags().contains(Message.Flag.UNSUPPORTED_MESSAGE)) {
                  UnsupportedMessageException ex = (UnsupportedMessageException)request.getBody();
                  response = request;
                  MessageServer.this.logger.error(ex.getMessage(), ex);
               } else {
                  MessageType type = request.getType();
                  Object body = request.getBody();
                  processor = (ServerProcessor)MessageServer.this.processors.get(type);
                  if(processor != null) {
                     try {
                        response = processor.process(new RequestContext(request.getId(), context.channel()), body);
                        response.trySetVersion(request.getVersion());
                     } catch (Exception var9) {
                        response = new Message(EnumSet.of(Message.Flag.FAILED_PROCESSOR), request.getId(), SystemMessageTypes.FAILED_PROCESSOR, new FailedProcessorException(var9.getClass(), var9.getMessage()));
                        response.trySetVersion(-1);
                        MessageServer.this.logger.error("Failed to process request: " + request, var9);
                     }
                  } else {
                     UnsupportedMessageException exx = new UnsupportedMessageException("Cannot find processor for message type: " + type);
                     response = new Message(EnumSet.of(Message.Flag.UNSUPPORTED_MESSAGE), request.getId(), SystemMessageTypes.UNSUPPORTED_MESSAGE, exx);
                     response.trySetVersion(-1);
                     MessageServer.this.logger.error(exx.getMessage());
                  }
               }

               this.sendResponse(context, response, processor);
            });
         } else {
            throw new IllegalStateException("Unknown message: " + input.getClass().getCanonicalName());
         }
      }

      public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
         MessageServer.this.logger.error(cause.getMessage(), cause);
      }

      private void sendResponse(ChannelHandlerContext context, Message response, ServerProcessor processor) {
         MessageServer.this.logger.trace("Sending response to request {} ", Long.valueOf(response.getId()));
         ChannelFuture f = context.writeAndFlush(response);
         f.awaitUninterruptibly();
         MessageServer.this.logger.trace("Sending response to request {} completed", Long.valueOf(response.getId()));
         if(processor != null) {
            processor.onComplete(response);
         }

         if(!f.isSuccess()) {
            Throwable cause = f.cause();
            if(cause instanceof ClosedChannelException) {
               MessageServer.this.logger.error("Cannot send response to {} due to closed channel!", context.channel().remoteAddress());
            } else if(cause != null) {
               MessageServer.this.logger.error(f.cause().getMessage(), f.cause());
            } else {
               MessageServer.this.logger.error("Cannot send response to {} due to unknown error!", context.channel().remoteAddress());
            }
         }

      }
   }

   private class ChannelStateHandler extends ChannelDuplexHandler {
      private ChannelStateHandler() {
      }

      public void channelActive(ChannelHandlerContext ctx) throws Exception {
         MessageServer.this.channels.add(ctx.channel());
      }

      public void channelInactive(ChannelHandlerContext ctx) throws Exception {
         MessageServer.this.channels.remove(ctx.channel());
      }
   }

   public static class Builder {
      private int acceptorThreads;
      private int workerThreads;
      private Optional<SSLOptions> sslOptions;
      private MessageCodec codec;
      private Map<MessageType, ServerProcessor> processors;

      private Builder() {
         this.acceptorThreads = FBUtilities.getAvailableProcessors();
         this.workerThreads = FBUtilities.getAvailableProcessors() * 8;
         this.sslOptions = Optional.empty();
         this.processors = new HashMap();
      }

      public MessageServer.Builder withAcceptorThreads(int acceptorThreads) {
         this.acceptorThreads = acceptorThreads;
         return this;
      }

      public MessageServer.Builder withWorkerThreads(int workerThreads) {
         this.workerThreads = workerThreads;
         return this;
      }

      public MessageServer.Builder withSSLOptions(Optional<SSLOptions> sslOptions) {
         this.sslOptions = sslOptions;
         return this;
      }

      public MessageServer.Builder withMessageCodec(MessageCodec codec) {
         this.codec = codec;
         return this;
      }

      public MessageServer.Builder withProcessor(MessageType type, ServerProcessor processor) {
         if(this.processors.putIfAbsent(type, processor) != null) {
            throw new IllegalArgumentException("Type already exists: " + type);
         } else {
            return this;
         }
      }

      public RemoteMessageServer buildRemote(String name, Iterable<InetAddress> listenAddresses, int port) {
         Preconditions.checkState(port > 0, "Invalid port number: " + port);
         Preconditions.checkState(StringUtils.isNotBlank(name), "Name cannot be empty or null");
         RemoteMessageServer server = new RemoteMessageServer(this.acceptorThreads, this.workerThreads, this.sslOptions, this.processors, this.codec, name, listenAddresses, port);
         return server;
      }

      public LocalMessageServer buildLocal(String name) {
         Preconditions.checkState(StringUtils.isNotBlank(name), "Name cannot be empty or null");
         LocalMessageServer server = new LocalMessageServer(this.acceptorThreads, this.workerThreads, this.sslOptions, this.processors, this.codec, name);
         return server;
      }
   }
}
