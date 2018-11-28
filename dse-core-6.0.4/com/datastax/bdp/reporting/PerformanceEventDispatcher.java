package com.datastax.bdp.reporting;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PerformanceEventDispatcher<T extends CqlWritable> {
   private static final Logger logger = LoggerFactory.getLogger(PerformanceEventDispatcher.class);
   private final String name;
   private final CqlWriter<T> writer;
   private final AtomicInteger asyncWriters;
   private final AtomicReference<ThreadPoolExecutor> poolHolder = new AtomicReference((Object)null);

   public PerformanceEventDispatcher(String name, CqlWriter<T> writer, int asyncWriters) {
      this.name = name;
      this.writer = writer;
      this.asyncWriters = new AtomicInteger(asyncWriters);
   }

   public synchronized void activate() {
      if(this.poolHolder.get() == null) {
         ThreadFactory threadFactory = (new ThreadFactoryBuilder()).setDaemon(true).setNameFormat(this.name + "-Thread-%d").build();
         ThreadPoolExecutor newPool = new ThreadPoolExecutor(this.asyncWriters.get(), this.asyncWriters.get(), 30L, TimeUnit.SECONDS, new LinkedBlockingQueue(), threadFactory);
         this.poolHolder.set(newPool);
         newPool.prestartAllCoreThreads();
      } else {
         logger.warn(this.name + " worker pool has already been activated!");
      }

   }

   public void submit(T writeable) {
      ExecutorService pool = (ExecutorService)this.poolHolder.get();
      if(pool != null) {
         Runnable writeTask = () -> {
            this.writer.write(writeable);
         };
         pool.submit(writeTask);
      }

   }

   public void shutdown() {
      ExecutorService pool = (ExecutorService)this.poolHolder.getAndSet(null);
      if(pool != null) {
         logger.info("Shutting down " + this.name + " worker pool...");
         pool.shutdown();

         try {
            if(pool.awaitTermination(30L, TimeUnit.SECONDS)) {
               logger.info("..." + this.name + " worker pool shutdown complete");
            } else {
               logger.info("Did not gracefully shutdown " + this.name + " worker pool within time limit!");
            }
         } catch (InterruptedException var3) {
            logger.info("Caught exception while waiting for " + this.name + " worker pool shutdown", var3);
         }
      }

   }

   public synchronized void setAsyncWriters(int asyncWriters) {
      this.asyncWriters.set(asyncWriters);
      ThreadPoolExecutor executor = (ThreadPoolExecutor)this.poolHolder.get();
      if(executor != null) {
         executor.setCorePoolSize(asyncWriters);
         executor.setMaximumPoolSize(asyncWriters);
         logger.debug("Using {} writers", Integer.valueOf(asyncWriters));
         executor.prestartAllCoreThreads();
      }

   }
}
