package org.apache.cassandra.concurrent;

import com.google.common.annotations.VisibleForTesting;
import io.reactivex.Scheduler.Worker;
import io.reactivex.disposables.CompositeDisposable;
import io.reactivex.disposables.Disposable;
import io.reactivex.internal.disposables.EmptyDisposable;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class IOScheduler extends StagedScheduler {
   @VisibleForTesting
   static final int MIN_POOL_SIZE = Integer.valueOf(System.getProperty("dse.io.sched.min_pool_size", "8")).intValue();
   @VisibleForTesting
   public static final int MAX_POOL_SIZE = Integer.valueOf(System.getProperty("dse.io.sched.max_pool_size", "256")).intValue();
   @VisibleForTesting
   static final int KEEP_ALIVE_TIME_SECS = Integer.valueOf(System.getProperty("dse.io.sched.keep_alive_secs", "5")).intValue();
   private final Function<ThreadFactory, ExecutorBasedWorker> workerSupplier;
   private final AtomicReference<IOScheduler.WorkersPool> pool;
   private final AtomicBoolean shutdown;
   private final int keepAliveMillis;

   IOScheduler() {
      this((factory) -> {
         return ExecutorBasedWorker.singleThreaded(factory);
      }, KEEP_ALIVE_TIME_SECS * 1000);
   }

   @VisibleForTesting
   IOScheduler(Function<ThreadFactory, ExecutorBasedWorker> workerSupplier, int keepAliveMillis) {
      this.shutdown = new AtomicBoolean(false);
      this.workerSupplier = workerSupplier;
      this.pool = new AtomicReference(new IOScheduler.WorkersPool(workerSupplier, keepAliveMillis));
      this.keepAliveMillis = keepAliveMillis;
   }

   public int metricsCoreId() {
      return TPC.getNumCores();
   }

   public void enqueue(TPCRunnable runnable) {
      (new IOScheduler.PooledTaskWorker((IOScheduler.WorkersPool)this.pool.get(), runnable)).execute();
   }

   public Disposable scheduleDirect(Runnable run, long delay, TimeUnit unit) {
      return super.scheduleDirect(TPCRunnable.wrap(run, delay != 0L?TPCTaskType.TIMED_UNKNOWN:TPCTaskType.UNKNOWN, TPC.getNextCore()), delay, unit);
   }

   public Disposable schedule(Runnable run, TPCTaskType stage, long delay, TimeUnit unit) {
      return super.scheduleDirect(TPCRunnable.wrap(run, stage, TPC.getNextCore()), delay, unit);
   }

   public boolean isOnScheduler(Thread thread) {
      return thread instanceof IOThread;
   }

   public Worker createWorker() {
      return new IOScheduler.PooledWorker((IOScheduler.WorkersPool)this.pool.get());
   }

   public void start() {
      while(true) {
         if(this.pool.get() == null) {
            IOScheduler.WorkersPool next = new IOScheduler.WorkersPool(this.workerSupplier, this.keepAliveMillis);
            if(!this.pool.compareAndSet(null, next)) {
               next.shutdown();
               continue;
            }
         }

         return;
      }
   }

   public void shutdown() {
      if(this.shutdown.compareAndSet(false, true)) {
         for(IOScheduler.WorkersPool current = (IOScheduler.WorkersPool)this.pool.get(); current != null; current = (IOScheduler.WorkersPool)this.pool.get()) {
            if(this.pool.compareAndSet(current, null)) {
               current.shutdown();
               break;
            }
         }

      }
   }

   public boolean isShutdown() {
      return this.shutdown.get();
   }

   @VisibleForTesting
   public int numCachedWorkers() {
      IOScheduler.WorkersPool pool = (IOScheduler.WorkersPool)this.pool.get();
      return pool == null?0:pool.cachedSize();
   }

   public String toString() {
      return String.format("IO scheduler: cached workers: %d", new Object[]{Integer.valueOf(this.numCachedWorkers())});
   }

   static class PooledWorker extends Worker {
      private final CompositeDisposable tasks;
      private final IOScheduler.WorkersPool pool;
      private final ExecutorBasedWorker worker;
      private final AtomicBoolean disposed;

      PooledWorker(IOScheduler.WorkersPool pool) {
         if(pool == null) {
            throw new RejectedExecutionException("Task sent to a shut down scheduler.");
         } else {
            this.tasks = new CompositeDisposable();
            this.pool = pool;
            this.worker = pool.get();
            this.disposed = new AtomicBoolean(this.worker == ExecutorBasedWorker.DISPOSED);
         }
      }

      public void dispose() {
         if(this.disposed.compareAndSet(false, true)) {
            this.tasks.dispose();
            this.pool.release(this.worker);
         }

      }

      public boolean isDisposed() {
         return this.disposed.get();
      }

      public Disposable schedule(Runnable action, long delayTime, TimeUnit unit) {
         return (Disposable)(this.disposed.get()?EmptyDisposable.INSTANCE:this.worker.scheduleActual(action, delayTime, unit, this.tasks));
      }
   }

   static final class PooledTaskWorker implements Runnable {
      private final IOScheduler.WorkersPool pool;
      private final ExecutorBasedWorker worker;
      private final Runnable task;

      PooledTaskWorker(IOScheduler.WorkersPool pool, Runnable task) {
         if(pool == null) {
            throw new RejectedExecutionException("Task sent to a shut down scheduler.");
         } else {
            this.pool = pool;
            this.worker = pool.get();
            this.task = task;
         }
      }

      public void execute() {
         this.worker.getExecutor().execute(this);
      }

      public void run() {
         try {
            this.task.run();
         } finally {
            this.pool.release(this.worker);
         }

      }
   }

   static final class WorkersPool {
      private final ThreadFactory threadFactory = new IOThread.Factory();
      private final Function<ThreadFactory, ExecutorBasedWorker> workerSupplier;
      private final Deque<ExecutorBasedWorker> workersQueue;
      private final CompositeDisposable allWorkers;
      private final AtomicBoolean shutdown;
      private final ScheduledFuture<?> killTimer;
      private final AtomicInteger workersQueueSize;

      WorkersPool(Function<ThreadFactory, ExecutorBasedWorker> workerSupplier, int keepAliveMillis) {
         this.workerSupplier = workerSupplier;
         this.workersQueue = new ConcurrentLinkedDeque();
         this.workersQueueSize = new AtomicInteger(0);
         this.allWorkers = new CompositeDisposable();
         this.shutdown = new AtomicBoolean(false);
         this.killTimer = this.startKillTimer(keepAliveMillis);
      }

      int cachedSize() {
         return this.workersQueueSize.get();
      }

      ExecutorBasedWorker get() {
         if(!this.shutdown.get() && !this.allWorkers.isDisposed()) {
            ExecutorBasedWorker threadWorker = (ExecutorBasedWorker)this.workersQueue.pollFirst();
            if(threadWorker != null) {
               this.workersQueueSize.decrementAndGet();
               return threadWorker;
            } else {
               ExecutorBasedWorker w = (ExecutorBasedWorker)this.workerSupplier.apply(this.threadFactory);
               this.allWorkers.add(w);
               return w;
            }
         } else {
            return ExecutorBasedWorker.DISPOSED;
         }
      }

      private ScheduledFuture<?> startKillTimer(int keepAliveMillis) {
         return IOScheduler.MAX_POOL_SIZE <= IOScheduler.MIN_POOL_SIZE?null:ScheduledExecutors.scheduledFastTasks.scheduleAtFixedRate(() -> {
            if(this.workersQueueSize.get() > IOScheduler.MIN_POOL_SIZE) {
               long runStartTime = System.currentTimeMillis();

               do {
                  ExecutorBasedWorker w = (ExecutorBasedWorker)this.workersQueue.peekLast();
                  if(w == null || w.unusedTime(runStartTime) < (long)keepAliveMillis || !this.workersQueue.removeLastOccurrence(w)) {
                     break;
                  }

                  w.dispose();
               } while(this.workersQueueSize.decrementAndGet() > IOScheduler.MIN_POOL_SIZE);

            }
         }, (long)keepAliveMillis, (long)keepAliveMillis, TimeUnit.MILLISECONDS);
      }

      void release(ExecutorBasedWorker worker) {
         if(!this.shutdown.get() && this.workersQueueSize.get() < IOScheduler.MAX_POOL_SIZE) {
            this.workersQueueSize.incrementAndGet();
            worker.markUse(System.currentTimeMillis());
            this.workersQueue.addFirst(worker);
         } else {
            worker.dispose();
         }

      }

      void shutdown() {
         if(this.shutdown.compareAndSet(false, true)) {
            if(this.killTimer != null) {
               this.killTimer.cancel(false);
            }

            this.allWorkers.dispose();
         }
      }
   }
}
