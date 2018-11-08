package com.datastax.bdp.node.transport;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableSet;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteMessageServer extends MessageServer {
   private static final Logger logger = LoggerFactory.getLogger(RemoteMessageServer.class);
   public final ImmutableCollection<InetAddress> addresses;
   public final int port;

   RemoteMessageServer(int acceptorThreads, int workerThreads, Optional<SSLOptions> sslOptions, Map<MessageType, ServerProcessor> processors, MessageCodec codec, String name, Iterable<InetAddress> addresses, int port) {
      super(acceptorThreads, workerThreads, sslOptions, processors, codec, name);
      this.addresses = ImmutableSet.copyOf(addresses);
      this.port = port;
   }

   protected ServerBootstrap bootstrap() {
      EventLoopGroup boss = new NioEventLoopGroup(this.acceptorThreads, new DefaultThreadFactory(String.format("%s RemoteMessageServer acceptor", new Object[]{this.name}), true));
      EventLoopGroup worker = new NioEventLoopGroup(Math.max(1, this.workerThreads / 4), new DefaultThreadFactory(String.format("%s RemoteMessageServer IO worker", new Object[]{this.name}), true));
      this.groups.add(boss);
      this.groups.add(worker);
      return ((ServerBootstrap)(new ServerBootstrap()).group(boss, worker).channel(NioServerSocketChannel.class)).childOption(ChannelOption.SO_KEEPALIVE, Boolean.valueOf(true));
   }

   protected Collection<ChannelFuture> doBind(ServerBootstrap bootstrap) {
      logger.info("Starting {} internal message server on {}:{}", new Object[]{this.name, this.addresses, Integer.valueOf(this.port)});
      return (Collection)this.addresses.stream().map((address) -> {
         return bootstrap.bind(new InetSocketAddress(address, this.port));
      }).collect(Collectors.toList());
   }
}
