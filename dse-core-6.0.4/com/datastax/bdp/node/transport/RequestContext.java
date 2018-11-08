package com.datastax.bdp.node.transport;

import io.netty.channel.Channel;
import java.net.InetSocketAddress;

public class RequestContext {
   private final Channel nettyChannel;
   private final long id;

   public RequestContext(long id, Channel nettyChannel) {
      this.id = id;
      this.nettyChannel = nettyChannel;
   }

   public long getId() {
      return this.id;
   }

   public InetSocketAddress getRemoteAddress() {
      return (InetSocketAddress)this.nettyChannel.remoteAddress();
   }
}
