package org.apache.cassandra.concurrent;

import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.EventLoop;
import java.util.concurrent.Executor;

@VisibleForTesting
public abstract class EventLoopBasedScheduler<E extends EventLoop> extends StagedScheduler {
   protected final E eventLoop;

   @VisibleForTesting
   public EventLoopBasedScheduler(E eventLoop) {
      assert eventLoop != null;

      this.eventLoop = eventLoop;
   }

   public void enqueue(TPCRunnable runnable) {
      this.eventLoop.execute(runnable);
   }

   public Executor getExecutor() {
      return this.eventLoop;
   }

   public ExecutorBasedWorker createWorker() {
      return ExecutorBasedWorker.withEventLoop(this.eventLoop);
   }
}
