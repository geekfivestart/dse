package org.apache.cassandra.concurrent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebuggableThreadPoolExecutor extends ThreadPoolExecutor implements TracingAwareExecutorService {
   protected static final Logger logger = LoggerFactory.getLogger(DebuggableThreadPoolExecutor.class);
   public static final RejectedExecutionHandler blockingExecutionHandler = new RejectedExecutionHandler() {
      public void rejectedExecution(Runnable task, ThreadPoolExecutor executor) {
         ((DebuggableThreadPoolExecutor)executor).onInitialRejection(task);
         BlockingQueue queue = executor.getQueue();

         while(!executor.isShutdown()) {
            try {
               if(queue.offer(task, 1000L, TimeUnit.MILLISECONDS)) {
                  ((DebuggableThreadPoolExecutor)executor).onFinalAccept(task);
                  return;
               }
            } catch (InterruptedException var5) {
               throw new AssertionError(var5);
            }
         }

         ((DebuggableThreadPoolExecutor)executor).onFinalRejection(task);
         throw new RejectedExecutionException("ThreadPoolExecutor has shut down");
      }
   };

   public DebuggableThreadPoolExecutor(String threadPoolName, int priority) {
      this(1, 2147483647L, TimeUnit.SECONDS, new LinkedBlockingQueue(), new NamedThreadFactory(threadPoolName, priority));
   }

   public DebuggableThreadPoolExecutor(int corePoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> queue, ThreadFactory factory) {
      this(corePoolSize, corePoolSize, keepAliveTime, unit, queue, factory);
   }

   public DebuggableThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
      this.allowCoreThreadTimeOut(true);
      this.setRejectedExecutionHandler(blockingExecutionHandler);
   }

   public static DebuggableThreadPoolExecutor createCachedThreadpoolWithMaxSize(String threadPoolName) {
      return new DebuggableThreadPoolExecutor(0, 2147483647, 60L, TimeUnit.SECONDS, new SynchronousQueue(), new NamedThreadFactory(threadPoolName));
   }

   public static DebuggableThreadPoolExecutor createWithFixedPoolSize(String threadPoolName, int size) {
      return createWithMaximumPoolSize(threadPoolName, size, 2147483647, TimeUnit.SECONDS);
   }

   public static DebuggableThreadPoolExecutor createWithFixedPoolSize(ThreadFactory threadFactory, int size) {
      return new DebuggableThreadPoolExecutor(size, 2147483647, 2147483647L, TimeUnit.SECONDS, new LinkedBlockingQueue(), threadFactory);
   }

   public static DebuggableThreadPoolExecutor createWithMaximumPoolSize(String threadPoolName, int size, int keepAliveTime, TimeUnit unit) {
      return new DebuggableThreadPoolExecutor(size, 2147483647, (long)keepAliveTime, unit, new LinkedBlockingQueue(), new NamedThreadFactory(threadPoolName));
   }

   protected void onInitialRejection(Runnable task) {
   }

   protected void onFinalAccept(Runnable task) {
   }

   protected void onFinalRejection(Runnable task) {
   }

   public void execute(Runnable command, ExecutorLocals locals) {
      super.execute((Runnable)(locals != null && !(command instanceof DebuggableThreadPoolExecutor.LocalSessionWrapper)?new DebuggableThreadPoolExecutor.LocalSessionWrapper(command, locals):command));
   }

   public void execute(Runnable command) {
      ExecutorLocals locals;
      super.execute((Runnable)(!(command instanceof DebuggableThreadPoolExecutor.LocalSessionWrapper) && (locals = ExecutorLocals.create()) != null?new DebuggableThreadPoolExecutor.LocalSessionWrapper(command, locals):command));
   }

   protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T result) {
      ExecutorLocals locals;
      return (RunnableFuture)(!(runnable instanceof DebuggableThreadPoolExecutor.LocalSessionWrapper) && (locals = ExecutorLocals.create()) != null?new DebuggableThreadPoolExecutor.LocalSessionWrapper(Executors.callable(runnable, result), locals):super.newTaskFor(runnable, result));
   }

   protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
      ExecutorLocals locals;
      return (RunnableFuture)(!(callable instanceof DebuggableThreadPoolExecutor.LocalSessionWrapper) && (locals = ExecutorLocals.create()) != null?new DebuggableThreadPoolExecutor.LocalSessionWrapper(callable, locals):super.newTaskFor(callable));
   }

   protected void afterExecute(Runnable r, Throwable t) {
      super.afterExecute(r, t);
      maybeResetTraceSessionWrapper(r);
      logExceptionsAfterExecute(r, t);
   }

   protected static void maybeResetTraceSessionWrapper(Runnable r) {
      if(r instanceof DebuggableThreadPoolExecutor.LocalSessionWrapper) {
         DebuggableThreadPoolExecutor.LocalSessionWrapper tsw = (DebuggableThreadPoolExecutor.LocalSessionWrapper)r;
         tsw.reset();
      }

   }

   protected void beforeExecute(Thread t, Runnable r) {
      if(r instanceof DebuggableThreadPoolExecutor.LocalSessionWrapper) {
         ((DebuggableThreadPoolExecutor.LocalSessionWrapper)r).setupContext();
      }

      super.beforeExecute(t, r);
   }

   public static void logExceptionsAfterExecute(Runnable r, Throwable t) {
      Throwable hiddenThrowable = extractThrowable(r);
      if(hiddenThrowable != null) {
         handleOrLog(hiddenThrowable);
      }

      if(t != null && Thread.getDefaultUncaughtExceptionHandler() == null) {
         handleOrLog(t);
      }

   }

   public static void handleOrLog(Throwable t) {
      if(Thread.getDefaultUncaughtExceptionHandler() == null) {
         logger.error("Error in ThreadPoolExecutor", t);
      } else {
         Thread.getDefaultUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), t);
      }

   }

   public static Throwable extractThrowable(Runnable runnable) {
      if(runnable instanceof Future && ((Future)runnable).isDone()) {
         try {
            ((Future)runnable).get();
         } catch (InterruptedException var2) {
            throw new AssertionError(var2);
         } catch (CancellationException var3) {
            logger.trace("Task cancelled", var3);
         } catch (ExecutionException var4) {
            return var4.getCause();
         }
      }

      return null;
   }

   private static class LocalSessionWrapper<T> implements RunnableFuture {
      private final ExecutorLocals locals;
      private final FutureTask<T> delegate;

      public LocalSessionWrapper(Callable<T> command, ExecutorLocals locals) {
         this.delegate = new FutureTask(command);
         this.locals = locals;
      }

      public LocalSessionWrapper(Runnable command, ExecutorLocals locals) {
         this.delegate = command instanceof FutureTask?(FutureTask)command:new FutureTask(command, (Object)null);
         this.locals = locals;
      }

      public boolean cancel(boolean mayInterruptIfRunning) {
         return this.delegate.cancel(mayInterruptIfRunning);
      }

      public Object get() throws InterruptedException, ExecutionException {
         return this.delegate.get();
      }

      public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
         return this.delegate.get(timeout, unit);
      }

      public boolean isCancelled() {
         return this.delegate.isCancelled();
      }

      public boolean isDone() {
         return this.delegate.isDone();
      }

      public void run() {
         this.delegate.run();
      }

      private void setupContext() {
         ExecutorLocals.set(this.locals);
      }

      private void reset() {
         ExecutorLocals.set((ExecutorLocals)null);
      }
   }
}
