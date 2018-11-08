package com.datastax.bdp.util;

import com.google.common.util.concurrent.MoreExecutors;
import java.io.Closeable;
import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.net.SocketFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SocketUtils {
   private static final Logger logger = LoggerFactory.getLogger(SocketUtils.class);
   public static final SocketUtils instance = new SocketUtils();

   private SocketUtils() {
   }

   public boolean tryConnect(SocketAddress socketAddress, Duration timeout) {
      try {
         Socket socket = SocketFactory.getDefault().createSocket();
         Throwable var4 = null;

         boolean var5;
         try {
            socket.setKeepAlive(false);
            socket.setTcpNoDelay(true);
            socket.setSoTimeout((int)timeout.toMillis());
            socket.connect(socketAddress);
            var5 = true;
         } catch (Throwable var16) {
            var4 = var16;
            throw var16;
         } finally {
            if(socket != null) {
               if(var4 != null) {
                  try {
                     socket.close();
                  } catch (Throwable var15) {
                     var4.addSuppressed(var15);
                  }
               } else {
                  socket.close();
               }
            }

         }

         return var5;
      } catch (ConnectException | SocketTimeoutException var18) {
         logger.trace("Could not connect to " + socketAddress, var18);
      } catch (IOException var19) {
         logger.debug("Failed to connect to " + socketAddress, var19);
      }

      return false;
   }

   public AsynchronousSocketChannel newChannelForConnectionTesting() throws IOException {
      AsynchronousSocketChannel channel = AsynchronousSocketChannel.open();
      channel.setOption(StandardSocketOptions.SO_KEEPALIVE, Boolean.valueOf(true));
      channel.setOption(StandardSocketOptions.TCP_NODELAY, Boolean.valueOf(true));
      return channel;
   }

   public CompletableFuture<AsynchronousSocketChannel> tryConnectAsync(final AsynchronousSocketChannel channel, SocketAddress socketAddress) throws IOException {
      try {
         final CompletableFuture<AsynchronousSocketChannel> future = new CompletableFuture();
         channel.connect(socketAddress, (Object)null, new CompletionHandler<Void, Object>() {
            public void completed(Void result, Object attachment) {
               future.complete(channel);
            }

            public void failed(Throwable exc, Object attachment) {
               future.completeExceptionally(exc);
            }
         });
         return future;
      } catch (RuntimeException var4) {
         this.closeNoThrow(channel, MoreExecutors.directExecutor());
         throw var4;
      }
   }

   public CompletableFuture<AsynchronousSocketChannel> tryConnectAsync(AsynchronousSocketChannel channel, SocketAddress address, Duration timeout, ScheduledExecutorService executor) throws IOException {
      if(!timeout.isZero()) {
         ScheduledFuture timeoutFuture = executor.schedule(() -> {
            this.closeNoThrow(channel, MoreExecutors.directExecutor());
         }, timeout.toNanos(), TimeUnit.NANOSECONDS);

         try {
            return this.tryConnectAsync(channel, address).handleAsync((ch, exc) -> {
               if(!timeoutFuture.isDone()) {
                  timeoutFuture.cancel(false);
               }

               if(timeoutFuture.isDone() && exc instanceof AsynchronousCloseException) {
                  throw new CompletionException(new SocketTimeoutException("Connection timed out"));
               } else if(exc != null) {
                  throw new CompletionException(exc);
               } else {
                  return ch;
               }
            }, executor);
         } catch (Error | IOException | RuntimeException var7) {
            timeoutFuture.cancel(false);
            throw var7;
         }
      } else {
         return this.tryConnectAsync(channel, address);
      }
   }

   public CompletableFuture<Long> tryConnectAsyncAndAwaitDisconnection(AsynchronousSocketChannel channel, SocketAddress address, Duration timeout, ScheduledExecutorService executor, Consumer<AsynchronousSocketChannel> onConnected) throws IOException {
      return this.tryConnectAsync(channel, address, timeout, executor).thenComposeAsync((ch) -> {
         if(onConnected != null) {
            onConnected.accept(ch);
         }

         return this.awaitDisconnection(ch, executor);
      }, executor);
   }

   public CompletableFuture<Long> awaitDisconnection(AsynchronousSocketChannel channel, Executor executor) {
      return this.skipData(channel).whenCompleteAsync((result, exc) -> {
         if(channel.isOpen()) {
            this.closeNoThrow(channel, MoreExecutors.directExecutor());
         }

      }, executor);
   }

   public CompletableFuture<Void> closeNoThrow(Closeable closeable, Executor executor) {
      return CompletableFuture.runAsync(() -> {
         try {
            closeable.close();
         } catch (IOException var2) {
            ;
         } catch (Throwable var3) {
            logger.error(var3.getMessage(), var3);
         }

      }, executor);
   }

   private CompletableFuture<Long> skipData(final AsynchronousSocketChannel channel) {
      final CompletableFuture<Long> future = new CompletableFuture();
      final ByteBuffer buf = ByteBuffer.allocate(32);
      channel.read(buf, Long.valueOf(0L), new CompletionHandler<Integer, Long>() {
         public void completed(Integer result, Long skippedSoFar) {
            if(result.intValue() >= 0) {
               try {
                  channel.read((ByteBuffer)buf.clear(), Long.valueOf(skippedSoFar.longValue() + (long)result.intValue()), this);
               } catch (Throwable var4) {
                  future.completeExceptionally(var4);
               }
            } else {
               future.complete(skippedSoFar);
            }

         }

         public void failed(Throwable exc, Long attachment) {
            future.completeExceptionally(exc);
         }
      });
      return future;
   }
}
