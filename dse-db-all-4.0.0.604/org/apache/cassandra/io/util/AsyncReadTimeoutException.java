package org.apache.cassandra.io.util;

import io.netty.util.internal.logging.InternalLogLevel;
import org.apache.cassandra.db.monitoring.AbortedOperationException;

public class AsyncReadTimeoutException extends AbortedOperationException {
   public AsyncReadTimeoutException(AsynchronousChannelProxy channel, Class caller) {
      super(String.format("Timed out async read from %s for file %s%s", new Object[]{caller.getCanonicalName(), channel.filePath, channel.epollChannel != null?", more information on epoll state with " + channel.epollChannel.getEpollEventLoop().epollFd() + " in the logs.":""}), (Throwable)null, false, false);
      if(channel.epollChannel != null) {
         channel.epollChannel.getEpollEventLoop().toLogAsync(InternalLogLevel.DEBUG);
      }

   }
}
