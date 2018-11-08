package com.datastax.bdp.node.transport;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalEventLoopGroup;
import io.netty.channel.local.LocalServerChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class LocalMessageServer extends MessageServer {
   LocalMessageServer(int acceptorThreads, int workerThreads, Optional<SSLOptions> sslOptions, Map<MessageType, ServerProcessor> processors, MessageCodec codec, String name) {
      super(acceptorThreads, workerThreads, sslOptions, processors, codec, name);
   }

   protected ServerBootstrap bootstrap() {
      EventLoopGroup boss = new LocalEventLoopGroup(this.acceptorThreads, new DefaultThreadFactory(String.format("%s LocalMessageServer acceptor", new Object[]{this.name}), true));
      EventLoopGroup worker = new LocalEventLoopGroup(Math.max(1, this.workerThreads / 4), new DefaultThreadFactory(String.format("%s LocalMessageServer io worker", new Object[]{this.name}), true));
      this.groups.add(boss);
      this.groups.add(worker);
      return (ServerBootstrap)(new ServerBootstrap()).group(boss, worker).channel(LocalServerChannel.class);
   }

   protected Collection<ChannelFuture> doBind(ServerBootstrap bootstrap) {
      return Collections.singletonList(bootstrap.bind(new LocalAddress(this.name)));
   }
}
