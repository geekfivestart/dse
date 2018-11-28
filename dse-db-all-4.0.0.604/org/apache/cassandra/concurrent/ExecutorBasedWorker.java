package org.apache.cassandra.concurrent;

import io.netty.channel.EventLoop;
import io.reactivex.Scheduler.Worker;
import io.reactivex.disposables.Disposable;
import io.reactivex.internal.disposables.DisposableContainer;
import io.reactivex.internal.disposables.EmptyDisposable;
import io.reactivex.internal.disposables.ListCompositeDisposable;
import io.reactivex.internal.schedulers.ScheduledRunnable;
import io.reactivex.plugins.RxJavaPlugins;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Callable;

class ExecutorBasedWorker extends Worker {
   static final ExecutorBasedWorker DISPOSED = disposed();
   private final ScheduledExecutorService executor;
   private final boolean shutdown;
   private final ListCompositeDisposable tasks;
   private volatile boolean disposed;
   private long unusedSince;

   protected ExecutorBasedWorker(ScheduledExecutorService executor, boolean shutdown) {
      this.executor = executor;
      this.shutdown = shutdown;
      this.tasks = new ListCompositeDisposable();
   }

   static ExecutorBasedWorker withEventLoop(EventLoop loop) {
      return new ExecutorBasedWorker(loop, false);
   }

   static ExecutorBasedWorker withExecutor(ScheduledExecutorService executorService) {
      return new ExecutorBasedWorker(executorService, true);
   }

   static ExecutorBasedWorker singleThreaded(ThreadFactory threadFactory) {
      return withExecutor(Executors.newScheduledThreadPool(1, threadFactory));
   }

   private static ExecutorBasedWorker disposed() {
      ExecutorBasedWorker ret = new ExecutorBasedWorker(Executors.newSingleThreadScheduledExecutor(), true);
      ret.dispose();
      return ret;
   }

   public Executor getExecutor() {
      return this.executor;
   }

   public void dispose() {
      if(!this.disposed) {
         this.disposed = true;
         this.tasks.dispose();
         if(this.shutdown) {
            this.executor.shutdown();
         }
      }

   }

   public boolean isDisposed() {
      return this.disposed;
   }

   public Disposable schedule(Runnable action) {
      return (Disposable)(this.disposed?EmptyDisposable.INSTANCE:this.scheduleActual(action, 0L, (TimeUnit)null, this.tasks));
   }

   public Disposable schedule(Runnable action, long delayTime, TimeUnit unit) {
      return (Disposable)(this.disposed?EmptyDisposable.INSTANCE:this.scheduleActual(action, delayTime, unit, this.tasks));
   }

   ScheduledRunnable scheduleActual(final Runnable decoratedRun, final long delayTime, final TimeUnit unit, final DisposableContainer parent) {
      final ScheduledRunnable sr = new ScheduledRunnable(decoratedRun, parent);
      if (parent != null && !parent.add((Disposable)sr)) {
         return sr;
      }
      try {
         Future<?> f;
         if (delayTime <= 0L) {
            f = this.executor.submit((Callable<?>)sr);
         }
         else {
            f = this.executor.schedule((Callable<Object>)sr, delayTime, unit);
         }
         sr.setFuture((Future)f);
      }
      catch (RejectedExecutionException ex) {
         RxJavaPlugins.onError((Throwable)ex);
      }
      return sr;
   }

   public long unusedTime(long currrentTime) {
      return currrentTime - this.unusedSince;
   }

   public void markUse(long currentTime) {
      this.unusedSince = currentTime;
   }
}
