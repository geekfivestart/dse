package com.datastax.bdp.plugin;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@DsePlugin(
   dependsOn = {}
)
public class ThreadPoolPlugin extends AbstractPlugin {
   private static final Logger logger = LoggerFactory.getLogger(ThreadPoolPlugin.class);
   private final ThreadPoolPluginBean bean;
   private ThreadGroup group;
   private ThreadPoolExecutor executor;
   private DeferringScheduler scheduler;
   private final AtomicLong dropCount = new AtomicLong();
   private long keepAliveNanos;

   @Inject
   public ThreadPoolPlugin(ThreadPoolPluginBean bean) {
      this.keepAliveNanos = TimeUnit.SECONDS.toNanos(30L);
      this.bean = bean;
      bean.addPropertyChangeListener("maxThreads", new PropertyChangeListener() {
         public void propertyChange(PropertyChangeEvent evt) {
            int newValue = ((Integer)evt.getNewValue()).intValue();
            ThreadPoolPlugin.logger.info("Adjusting max threads to {} from {}", Integer.valueOf(newValue), evt.getOldValue());
            if(ThreadPoolPlugin.this.executor != null) {
               ThreadPoolPlugin.this.executor.setMaximumPoolSize(newValue);
            }

         }
      });
      bean.addPropertyChangeListener("coreThreads", new PropertyChangeListener() {
         public void propertyChange(PropertyChangeEvent evt) {
            int newValue = ((Integer)evt.getNewValue()).intValue();
            ThreadPoolPlugin.logger.info("Adjusting core threads to {} from {}", Integer.valueOf(newValue), evt.getOldValue());
            if(ThreadPoolPlugin.this.executor != null) {
               ThreadPoolPlugin.this.executor.setCorePoolSize(newValue);
            }

         }
      });
      bean.setPlugin(this);
   }

   public ThreadPoolPluginBean getBean() {
      return this.bean;
   }

   public synchronized void onActivate() {
      if(!this.isShutdown()) {
         throw new IllegalStateException("Already activated");
      } else {
         String threadGroupName = "PO-thread";
         this.group = new ThreadGroup("PO-threads");
         this.dropCount.set(0L);
         int coreThreads = this.bean.getCoreThreads();
         int maxThreads = Math.max(coreThreads, this.bean.getMaxThreads());
         int queueCapacity = this.bean.getQueueCapacity();
         logger.info("Starting pool with {} core threads ({} max) with queue size: {}", new Object[]{Integer.valueOf(coreThreads), Integer.valueOf(maxThreads), Integer.valueOf(queueCapacity)});
         this.executor = new ThreadPoolExecutor(coreThreads, maxThreads, this.keepAliveNanos, TimeUnit.NANOSECONDS, (BlockingQueue)(queueCapacity == 0?new SynchronousQueue():new ArrayBlockingQueue(queueCapacity)), (new ThreadFactoryBuilder()).setNameFormat("PO-thread-%d").setThreadFactory((r) -> {
            logger.debug("Adding thread #{}", Integer.valueOf(this.group.activeCount() + 1));
            return new Thread(this.group, r);
         }).build(), (r, executor1) -> {
            long count = this.dropCount.getAndIncrement();
            String msg = "Some performance service background tasks were not executed due to server load. This means some performance data may not be up-to-date. This may be fixed by disabling or reconfiguring some services.";
            if(count == 0L) {
               logger.warn("Some performance service background tasks were not executed due to server load. This means some performance data may not be up-to-date. This may be fixed by disabling or reconfiguring some services.");
            }

            throw new RejectedExecutionException("Some performance service background tasks were not executed due to server load. This means some performance data may not be up-to-date. This may be fixed by disabling or reconfiguring some services.");
         }) {
            protected void beforeExecute(Thread t, Runnable r) {
               if(ThreadPoolPlugin.logger.isDebugEnabled()) {
                  ThreadPoolPlugin.logger.debug("Executing task #{}", Integer.valueOf(r.hashCode()));
               }

            }
         };
         this.executor.prestartAllCoreThreads();
         this.scheduler = new DeferringScheduler(this.executor, (r) -> {
            return new Thread(this.group, r, "PO-thread scheduler");
         });

         while(this.executor.getPoolSize() < coreThreads || this.executor.getActiveCount() > 0) {
            try {
               TimeUnit.MILLISECONDS.sleep(100L);
            } catch (InterruptedException var6) {
               logger.warn("Executor interrupted during startup", var6);
            }
         }

      }
   }

   public long getDropCount() {
      return this.dropCount.get();
   }

   public int getThreadCount() {
      return this.executor.getPoolSize();
   }

   public void setKeepAliveTime(long time, TimeUnit unit) {
      this.keepAliveNanos = TimeUnit.NANOSECONDS.convert(time, unit);
      if(this.executor != null) {
         this.executor.setKeepAliveTime(time, unit);
      }

   }

   public synchronized void onPreDeactivate() {
      this.checkActivated();
      this.scheduler.shutdown();
      this.executor.shutdown();
      String groupName = this.group.getName();

      try {
         if(this.scheduler.awaitTermination(this.keepAliveNanos, TimeUnit.NANOSECONDS) && this.executor.awaitTermination(this.keepAliveNanos, TimeUnit.NANOSECONDS)) {
            for(int i = 0; i < 20 && this.group.activeCount() > 0; ++i) {
               TimeUnit.MILLISECONDS.sleep(100L);
            }

            if(this.group.activeCount() == 0) {
               this.group.destroy();
               return;
            }
         }

         logger.warn("Gave up waiting termination of {} ", groupName);
      } catch (IllegalThreadStateException var3) {
         logger.warn("Attempt to destroy thread group " + groupName + " with " + this.group.activeCount() + " active threads", var3);
      } catch (InterruptedException var4) {
         logger.warn("Interrupted while shutting down " + groupName, var4);
      }

   }

   private boolean isShutdown() {
      return this.executor == null || this.executor.isShutdown();
   }

   public void execute(Runnable command) throws RejectedExecutionException {
      this.checkActivated();
      this.executor.execute(command);
   }

   public <T> Future<T> submit(Callable<T> task) {
      this.checkActivated();
      return this.scheduler.submit(task);
   }

   private void checkActivated() {
      if(this.isShutdown()) {
         throw new IllegalStateException("plugin not activated");
      }
   }

   public Future<?> submit(Runnable task) {
      return this.submit(task, (Object)null);
   }

   public <T> Future<T> submit(Runnable task, T futureResult) {
      this.checkActivated();
      return this.executor.submit(task, futureResult);
   }

   public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
      this.checkActivated();
      return this.scheduler.schedule(callable, delay, unit);
   }

   public ScheduledFuture<?> schedule(Runnable task, long delay, TimeUnit unit) {
      this.checkActivated();
      return this.scheduler.schedule(task, delay, unit);
   }

   public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
      this.checkActivated();
      return this.scheduler.scheduleAtFixedRate(command, initialDelay, period, unit);
   }

   public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
      this.checkActivated();
      return this.scheduler.scheduleWithFixedDelay(command, initialDelay, delay, unit);
   }

   public int getActiveCount() {
      return this.executor == null?0:this.executor.getActiveCount();
   }

   public long getRetryDelay() {
      return this.scheduler.getRetryDelay();
   }

   public void setRetryDelay(long retryDelay) {
      this.scheduler.setRetryDelay(retryDelay);
   }
}
