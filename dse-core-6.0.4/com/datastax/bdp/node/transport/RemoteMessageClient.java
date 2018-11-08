package com.datastax.bdp.node.transport;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.net.InetSocketAddress;
import java.util.Optional;
import org.apache.cassandra.gms.Gossiper;

public class RemoteMessageClient extends MessageClient {
   RemoteMessageClient(int maxConnections, int workerThreads, int handshakeTimeoutSecs, Optional<SSLOptions> sslOptions, MessageCodec codec) {
      super(maxConnections, workerThreads, handshakeTimeoutSecs, sslOptions, codec);
   }

   protected Bootstrap doSetup() {
      Gossiper.instance.register(new MessageClient.RequestTerminator());
      EventLoopGroup worker = new NioEventLoopGroup(Math.max(1, this.workerThreads / 2), new DefaultThreadFactory("RemoteMessageClient worker", true));
      this.groups.add(worker);
      return (Bootstrap)((Bootstrap)(new Bootstrap()).group(worker)).channel(NioSocketChannel.class);
   }

   protected ChannelFuture doConnect(Bootstrap bootstrap, String address, int port) {
      return bootstrap.connect(new InetSocketAddress(address, port));
   }
}
