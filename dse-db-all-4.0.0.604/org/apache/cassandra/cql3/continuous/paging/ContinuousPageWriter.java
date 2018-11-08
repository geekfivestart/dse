package org.apache.cassandra.cql3.continuous.paging;

import com.google.common.util.concurrent.RateLimiter;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.cassandra.transport.Frame;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ContinuousPageWriter {
   private static final Logger logger = LoggerFactory.getLogger(ContinuousPageWriter.class);
   private final int queueSizeInPages;
   private final ArrayBlockingQueue<Frame> pages;
   private final ContinuousPageWriter.Writer writer;

   ContinuousPageWriter(Supplier<Channel> channel, int maxPagesPerSecond, int queueSizeInPages) {
      assert queueSizeInPages >= 1 : "queue size must be at least one";

      this.queueSizeInPages = queueSizeInPages;
      this.pages = new ArrayBlockingQueue(queueSizeInPages);
      this.writer = new ContinuousPageWriter.Writer((Channel)channel.get(), this.pages, maxPagesPerSecond);
   }

   CompletableFuture<Void> completionFuture() {
      return this.writer.completionFuture;
   }

   void sendPage(Frame frame, boolean hasMorePages) {
      if(this.writer.canceled) {
         logger.trace("Discarding page because writer was cancelled");
         frame.release();
      } else {
         assert !this.writer.completed() : "Received unexpected page when writer was already completed";

         try {
            this.pages.add(frame);
         } catch (Throwable var4) {
            logger.warn("Failed to add continuous paging result to queue: {}", var4.getMessage());
            frame.release();
            throw var4;
         }

         if(!hasMorePages) {
            this.writer.complete();
         } else {
            this.writer.schedule(0L);
         }

      }
   }

   boolean hasSpace() {
      return this.pages.size() < this.queueSizeInPages;
   }

   boolean halfQueueAvailable() {
      return this.pages.size() < this.queueSizeInPages / 2 + 1;
   }

   int pendingPages() {
      return this.pages.size();
   }

   void sendError(Frame error) {
      this.writer.setError(error);
   }

   public void cancel(Frame error) {
      this.writer.cancel(error);
   }

   public boolean completed() {
      return this.writer.completed();
   }

   private static final class Writer implements Runnable {
      private final Channel channel;
      private final ArrayBlockingQueue<Frame> queue;
      private final AtomicBoolean completed;
      private final AtomicReference<Frame> error;
      private final RateLimiter limiter;
      private final CompletableFuture<Void> completionFuture;
      private volatile boolean canceled;

      public Writer(Channel channel, ArrayBlockingQueue<Frame> queue, int maxPagesPerSecond) {
         this.channel = channel;
         this.queue = queue;
         this.completed = new AtomicBoolean(false);
         this.error = new AtomicReference((Object)null);
         this.limiter = RateLimiter.create(maxPagesPerSecond > 0?(double)maxPagesPerSecond:1.7976931348623157E308D);
         this.completionFuture = new CompletableFuture();
         channel.closeFuture().addListener((future) -> {
            if(ContinuousPageWriter.logger.isTraceEnabled()) {
               ContinuousPageWriter.logger.trace("Socket {} closed by the client", channel);
            }

            this.cancel((Frame)null);
         });
      }

      public void cancel(@Nullable Frame error) {
         if(!this.canceled) {
            ContinuousPageWriter.logger.trace("Cancelling continuous page writer");
            this.canceled = true;
            if(error != null && !this.error.compareAndSet((Object)null, error)) {
               ContinuousPageWriter.logger.debug("Failed to set final error when cancelling session, another error was already there");
            }

            this.complete();
         }
      }

      public void complete() {
         this.completed.compareAndSet(false, true);
         this.schedule(0L);
      }

      public boolean completed() {
         return this.completed.get();
      }

      public void schedule(long pauseMicros) {
         if(pauseMicros > 0L) {
            this.channel.eventLoop().schedule(this, pauseMicros, TimeUnit.MICROSECONDS);
         } else if(this.channel.eventLoop().inEventLoop()) {
            this.run();
         } else {
            this.channel.eventLoop().execute(this);
         }

      }

      public void setError(Frame error) {
         if(!this.completed()) {
            if(this.error.compareAndSet((Object)null, error)) {
               this.complete();
            }
         } else {
            ContinuousPageWriter.logger.warn("Got continuous paging error for client but writer was already completed, so could not pass it to the client");
         }

      }

      public boolean aborted() {
         return this.canceled || this.error.get() != null;
      }

      public void run() {
         try {
            this.processPendingPages();
            if(this.completed() && !this.completionFuture.isDone() && this.queue.isEmpty()) {
               if(this.error.get() != null) {
                  this.sendError();
               }

               this.completionFuture.complete((Object)null);
            }
         } catch (Throwable var2) {
            JVMStabilityInspector.inspectThrowable(var2);
            ContinuousPageWriter.logger.error("Error processing pages in Netty event loop: {}", var2);
         }

      }

      private void sendError() {
         if(ContinuousPageWriter.logger.isTraceEnabled()) {
            ContinuousPageWriter.logger.trace("Sending continuous paging error to client");
         }

         this.channel.write(this.error.get());
         this.channel.flush();
      }

      private void processPendingPages() {
         long pauseMicros = 1L;

         for(boolean aborted = this.aborted(); aborted || this.channel.isWritable(); aborted = this.aborted()) {
            if(!aborted && !this.limiter.tryAcquire()) {
               long intervalMicros = (long)((double)TimeUnit.SECONDS.toMicros(1L) / this.limiter.getRate());
               pauseMicros = intervalMicros / 10L;
               break;
            }

            Frame page = (Frame)this.queue.poll();
            if(page == null) {
               break;
            }

            this.processPage(page);
         }

         if(!this.queue.isEmpty()) {
            this.schedule(pauseMicros);
         }

      }

      private void processPage(Frame frame) {
         if(this.aborted()) {
            frame.release();
         } else {
            this.channel.write(frame);
            this.channel.flush();
         }

      }
   }
}
