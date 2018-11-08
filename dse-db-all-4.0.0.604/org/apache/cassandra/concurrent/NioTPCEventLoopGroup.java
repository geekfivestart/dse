package org.apache.cassandra.concurrent;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.nio.NioEventLoop;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.AbstractEventExecutor;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ScheduledFuture;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public class NioTPCEventLoopGroup extends NioEventLoopGroup implements TPCEventLoopGroup {
   private final ImmutableList<NioTPCEventLoopGroup.SingleCoreEventLoop> eventLoops = ImmutableList.copyOf(Iterables.transform(this, (e) -> {
      return (NioTPCEventLoopGroup.SingleCoreEventLoop)e;
   }));

   NioTPCEventLoopGroup(int nThreads) {
      super(nThreads, TPCThread.newTPCThreadFactory());
   }

   public ImmutableList<? extends TPCEventLoop> eventLoops() {
      return this.eventLoops;
   }

   protected EventLoop newChild(Executor executor, Object... args) throws Exception {
      assert executor instanceof TPCThread.TPCThreadsCreator;

      return new NioTPCEventLoopGroup.SingleCoreEventLoop(super.newChild(executor, args), (TPCThread.TPCThreadsCreator)executor);
   }

   public void setIoRatio(int ioRatio) {
      Iterator var2 = this.iterator();

      while(var2.hasNext()) {
         EventExecutor eventExecutor = (EventExecutor)var2.next();
         NioTPCEventLoopGroup.SingleCoreEventLoop eventLoop = (NioTPCEventLoopGroup.SingleCoreEventLoop)eventExecutor;
         ((NioEventLoop)eventLoop.nettyLoop).setIoRatio(ioRatio);
      }

   }

   private static class SingleCoreEventLoop extends AbstractEventExecutor implements TPCEventLoop {
      private final EventLoop nettyLoop;
      private final TPCThread thread;

      private SingleCoreEventLoop(EventLoop nettyLoop, TPCThread.TPCThreadsCreator executor) {
         super(nettyLoop.parent());
         this.nettyLoop = nettyLoop;
         Futures.getUnchecked(nettyLoop.submit(() -> {
         }));
         this.thread = executor.lastCreatedThread();

         assert this.thread != null;

      }

      public TPCThread thread() {
         return this.thread;
      }

      public TPCEventLoopGroup parent() {
         return (TPCEventLoopGroup)super.parent();
      }

      public EventLoop next() {
         return (EventLoop)super.next();
      }

      public ChannelFuture register(Channel channel) {
         return this.nettyLoop.register(channel);
      }

      public ChannelFuture register(ChannelPromise channelPromise) {
         return this.nettyLoop.register(channelPromise);
      }

      public ChannelFuture register(Channel channel, ChannelPromise channelPromise) {
         return this.nettyLoop.register(channel, channelPromise);
      }

      public boolean isShuttingDown() {
         return this.nettyLoop.isShuttingDown();
      }

      public Future<?> shutdownGracefully(long l, long l1, TimeUnit timeUnit) {
         return this.nettyLoop.shutdownGracefully(l, l1, timeUnit);
      }

      public Future<?> terminationFuture() {
         return this.nettyLoop.terminationFuture();
      }

      public void shutdown() {
         this.nettyLoop.shutdown();
      }

      public boolean isShutdown() {
         return this.nettyLoop.isShutdown();
      }

      public boolean isTerminated() {
         return this.nettyLoop.isTerminated();
      }

      public boolean awaitTermination(long l, TimeUnit timeUnit) throws InterruptedException {
         return this.nettyLoop.awaitTermination(l, timeUnit);
      }

      public boolean inEventLoop(Thread thread) {
         return this.nettyLoop.inEventLoop();
      }

      public boolean canExecuteImmediately(TPCTaskType taskType) {
         return this.coreId() == TPC.getCoreId();
      }

      public void execute(Runnable runnable) {
         this.nettyLoop.execute(runnable);
      }

      public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
         return this.nettyLoop.schedule(command, delay, unit);
      }

      public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
         return this.nettyLoop.schedule(callable, delay, unit);
      }

      public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
         return this.nettyLoop.scheduleAtFixedRate(command, initialDelay, period, unit);
      }

      public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
         return this.nettyLoop.scheduleWithFixedDelay(command, initialDelay, delay, unit);
      }
   }
}
