package com.datastax.bdp.node.transport;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.Optional;

public class LocalMessageClient extends MessageClient {
   private final String name;

   LocalMessageClient(int maxConnections, int workerThreads, int handshakeTimeoutSecs, Optional<SSLOptions> sslOptions, MessageCodec codec, String name) {
      super(maxConnections, workerThreads, handshakeTimeoutSecs, sslOptions, codec);
      this.name = name;
   }

   protected Bootstrap doSetup() {
      EventLoopGroup worker = new LocalEventLoopGroup(Math.max(1, this.workerThreads / 2), new DefaultThreadFactory(String.format("%s LocalMessageClient worker", new Object[]{this.name}), true));
      this.groups.add(worker);
      return (Bootstrap)((Bootstrap)(new Bootstrap()).group(worker)).channel(LocalChannel.class);
   }

   protected ChannelFuture doConnect(Bootstrap bootstrap, String ignoredAddress, int ignoredPort) {
      return bootstrap.connect(new LocalAddress(this.name));
   }
}
