package org.apache.cassandra.concurrent;

import io.netty.channel.EventLoop;

public interface TPCEventLoop extends EventLoop {
   default void start() {
   }

   TPCThread thread();

   default int coreId() {
      return this.thread().coreId();
   }

   boolean canExecuteImmediately(TPCTaskType var1);

   default boolean shouldBackpressure(boolean remote) {
      return false;
   }

   TPCEventLoopGroup parent();
}
