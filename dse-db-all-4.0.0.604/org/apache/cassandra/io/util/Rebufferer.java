package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCRunnable;
import org.apache.cassandra.concurrent.TPCScheduler;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.concurrent.TPCTimeoutTask;
import org.apache.cassandra.utils.NoSpamLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface Rebufferer extends ReaderFileProxy {
   Rebufferer.BufferHolder EMPTY = new Rebufferer.BufferHolder() {
      final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

      public ByteBuffer buffer() {
         return this.EMPTY_BUFFER;
      }

      public long offset() {
         return 0L;
      }

      public void release() {
      }
   };

   default Rebufferer.BufferHolder rebuffer(long position) {
      return this.rebuffer(position, Rebufferer.ReaderConstraint.NONE);
   }

   CompletableFuture<Rebufferer.BufferHolder> rebufferAsync(long var1);

   int rebufferSize();

   void closeReader();

   Rebufferer.BufferHolder rebuffer(long var1, Rebufferer.ReaderConstraint var3);

   public interface BufferHolder {
      ByteBuffer buffer();

      long offset();

      void release();
   }

   public static class NotInCacheException extends RuntimeException {
      private static final Logger logger = LoggerFactory.getLogger(Rebufferer.NotInCacheException.class);
      private static final NoSpamLogger noSpamLogger;
      private static final long serialVersionUID = 1L;
      public static final boolean DEBUG;
      private final AsynchronousChannelProxy channel;
      private final CompletableFuture<Void> cacheReady;

      public NotInCacheException(AsynchronousChannelProxy channel, CompletableFuture<Void> cacheReady, String path, long position) {
         super("Requested data (" + path + "@" + position + ") is not in cache.");
         this.channel = channel;
         this.cacheReady = cacheReady;
      }

      public synchronized Throwable fillInStackTrace() {
         return (Throwable)(DEBUG?super.fillInStackTrace():this);
      }

      public void accept(Class caller, Runnable onReady, Function<Throwable, Void> onError, TPCScheduler scheduler) {
         assert this.cacheReady != null;

         if(this.cacheReady.isDone() && !this.cacheReady.isCompletedExceptionally()) {
            onReady.run();
         } else {
            TPCRunnable wrappedOnReady = TPCRunnable.wrap(onReady, ExecutorLocals.create(), TPCTaskType.READ_DISK_ASYNC, scheduler);
            TPCTimeoutTask<Rebufferer.NotInCacheException.TimeoutPayload> timeout = new TPCTimeoutTask(new Rebufferer.NotInCacheException.TimeoutPayload(this.channel, this.cacheReady));
            timeout.submit((payload) -> {
               AsyncReadTimeoutException ex = new AsyncReadTimeoutException(payload.channel, caller);
               noSpamLogger.warn(ex.getMessage(), new Object[0]);
               payload.cacheReady.completeExceptionally(ex);
            }, (long)TPC.READ_ASYNC_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            this.cacheReady.whenComplete((ignored, ex) -> {
               timeout.dispose();

               try {
                  if(ex == null) {
                     scheduler.execute(wrappedOnReady);
                  }
               } catch (Throwable var7) {
                  ex = var7;
               }

               if(ex != null) {
                  wrappedOnReady.cancelled();
                  onError.apply(ex);
               }

            });
         }

      }

      public String toString() {
         return "NotInCache " + this.cacheReady;
      }

      static {
         noSpamLogger = NoSpamLogger.getLogger(logger, 1L, TimeUnit.MINUTES);
         DEBUG = Boolean.parseBoolean(System.getProperty("dse.debug_cache_misses", "false"));
         if(DEBUG) {
            logger.warn("NotInCacheException DEBUG is ON, performance will be impacted!!!");
         }

      }

      private static class TimeoutPayload {
         public final AsynchronousChannelProxy channel;
         public final CompletableFuture<Void> cacheReady;

         public TimeoutPayload(AsynchronousChannelProxy channel, CompletableFuture<Void> cacheReady) {
            this.channel = channel;
            this.cacheReady = cacheReady;
         }
      }
   }

   public static enum ReaderConstraint {
      NONE,
      ASYNC;

      private ReaderConstraint() {
      }
   }
}
