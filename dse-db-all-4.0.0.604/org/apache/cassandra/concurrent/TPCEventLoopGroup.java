package org.apache.cassandra.concurrent;

import com.google.common.collect.ImmutableList;
import io.netty.channel.EventLoopGroup;

public interface TPCEventLoopGroup extends EventLoopGroup {
   ImmutableList<? extends TPCEventLoop> eventLoops();
}
