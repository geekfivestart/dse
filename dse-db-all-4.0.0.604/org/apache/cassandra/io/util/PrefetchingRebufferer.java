package org.apache.cassandra.io.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.Meter;
import org.apache.cassandra.metrics.MetricNameFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrefetchingRebufferer extends WrappingRebufferer {
   private static final Logger logger = LoggerFactory.getLogger(PrefetchingRebufferer.class);
   private static final String READ_AHEAD_SIZE_KB_FROM_OPERATOR = System.getProperty("dse.read_ahead_size_kb", "");
   public static final int READ_AHEAD_SIZE_KB;
   private static final double READ_AHEAD_WINDOW = Double.parseDouble(System.getProperty("dse.read_ahead_window", "0.5"));
   public static final boolean READ_AHEAD_VECTORED = Boolean.parseBoolean(System.getProperty("dse.read_ahead_vectored", "true"));
   private final AsynchronousChannelProxy channel;
   private final Deque<PrefetchingRebufferer.PrefetchedEntry> queue;
   private final int prefetchSize;
   private final int windowSize;
   private final int alignmentMask;
   @VisibleForTesting
   public static final PrefetchingRebufferer.PrefetchingMetrics metrics;

   public PrefetchingRebufferer(Rebufferer source, AsynchronousChannelProxy channel) {
      this(source, channel, READ_AHEAD_SIZE_KB * 1024, READ_AHEAD_WINDOW);
   }

   PrefetchingRebufferer(Rebufferer source, AsynchronousChannelProxy channel, int readHeadSize, double window) {
      super(source);
      if(readHeadSize <= 0) {
         throw new IllegalArgumentException("Invalid read-ahead size: " + readHeadSize);
      } else if(window >= 0.0D && window <= 1.0D) {
         this.channel = channel;
         this.prefetchSize = (int)Math.ceil((double)readHeadSize / (double)source.rebufferSize());
         this.windowSize = (int)Math.ceil(window * (double)this.prefetchSize);
         this.queue = new ArrayDeque(this.prefetchSize);

         assert Integer.bitCount(source.rebufferSize()) == 1 : String.format("%d must be a power of two", new Object[]{Integer.valueOf(source.rebufferSize())});

         this.alignmentMask = -source.rebufferSize();
         if(logger.isTraceEnabled()) {
            logger.trace("{}: prefetch {}, window {}", new Object[]{channel.filePath(), Integer.valueOf(this.prefetchSize), Integer.valueOf(this.windowSize)});
         }

      } else {
         throw new IllegalArgumentException("Invalid window size, should be >= 0 and <=1: " + window);
      }
   }

   public Rebufferer.BufferHolder rebuffer(long position, Rebufferer.ReaderConstraint rc) {
      CompletableFuture<Rebufferer.BufferHolder> fut = this.rebufferAsync(position);
      if(rc != Rebufferer.ReaderConstraint.NONE && !fut.isDone()) {
         throw new Rebufferer.NotInCacheException(this.channel(), fut.thenAccept(Rebufferer.BufferHolder::release), this.channel().filePath, position);
      } else {
         try {
            return (Rebufferer.BufferHolder)fut.join();
         } catch (Throwable var6) {
            Throwables.propagateIfInstanceOf(var6.getCause(), CorruptSSTableException.class);
            throw Throwables.propagate(var6);
         }
      }
   }

   public CompletableFuture<Rebufferer.BufferHolder> rebufferAsync(long position) {
      long pageAlignedPos = position & (long)this.alignmentMask;

      PrefetchingRebufferer.PrefetchedEntry entry;
      for(entry = (PrefetchingRebufferer.PrefetchedEntry)this.queue.poll(); entry != null && entry.position < pageAlignedPos; entry = (PrefetchingRebufferer.PrefetchedEntry)this.queue.poll()) {
         entry.release();
      }

      CompletableFuture ret;
      if(entry == null) {
         ret = super.rebufferAsync(pageAlignedPos);
         this.prefetch(pageAlignedPos + (long)this.source.rebufferSize());
         return ret;
      } else if(entry.position == pageAlignedPos) {
         ret = entry.future;
         if(!entry.future.isDone()) {
            metrics.notReady.mark();
         }

         this.prefetch(pageAlignedPos + (long)this.source.rebufferSize());
         return ret;
      } else {
         assert entry.position > pageAlignedPos;

         this.queue.addFirst(entry);
         ret = super.rebufferAsync(pageAlignedPos);
         if(!ret.isDone()) {
            metrics.skipped.mark();
         }

         return ret;
      }
   }

   private void prefetch(long pageAlignedPosition) {
      assert this.queue.isEmpty() || pageAlignedPosition == ((PrefetchingRebufferer.PrefetchedEntry)this.queue.peekFirst()).position : String.format("Unexpected read-ahead position %d, first: %s, last: %s", new Object[]{Long.valueOf(pageAlignedPosition), this.queue.peekFirst(), this.queue.peekLast()});

      long firstPositionToPrefetch = this.queue.isEmpty()?pageAlignedPosition:((PrefetchingRebufferer.PrefetchedEntry)this.queue.peekLast()).position + (long)this.source.rebufferSize();
      int toPrefetch = this.prefetchSize - this.queue.size();
      if(toPrefetch >= this.windowSize) {
         this.channel.startBatch();

         try {
            for(int i = 0; i < toPrefetch; ++i) {
               long prefetchPosition = firstPositionToPrefetch + (long)(i * this.source.rebufferSize());
               if(prefetchPosition >= this.source.fileLength()) {
                  break;
               }

               this.queue.addLast(new PrefetchingRebufferer.PrefetchedEntry(prefetchPosition, super.rebufferAsync(prefetchPosition)));
            }
         } finally {
            this.channel.submitBatch();
         }

      }
   }

   public void close() {
      assert this.queue.isEmpty() : "Prefetched buffers should have been released";

      try {
         this.channel.close();
      } finally {
         super.close();
      }

   }

   public void closeReader() {
      this.releaseBuffers();

      try {
         this.channel.close();
      } finally {
         super.closeReader();
      }

   }

   private void releaseBuffers() {
      this.queue.forEach(PrefetchingRebufferer.PrefetchedEntry::release);
      this.queue.clear();
   }

   public String toString() {
      return String.format("Prefetching rebufferer: (%d/%d) buffers read-ahead, %d buffer size", new Object[]{Integer.valueOf(this.prefetchSize), Integer.valueOf(this.windowSize), Integer.valueOf(this.source.rebufferSize())});
   }

   static {
      DiskOptimizationStrategy diskOptimizationStrategy = DatabaseDescriptor.getDiskOptimizationStrategy();
      READ_AHEAD_SIZE_KB = READ_AHEAD_SIZE_KB_FROM_OPERATOR.isEmpty()?diskOptimizationStrategy.readAheadSizeKb():Integer.parseInt(READ_AHEAD_SIZE_KB_FROM_OPERATOR);
      logger.info("Read ahead for sequential reads (e.g. range queries, compactions) is {} k-bytes, window: {}, vectored: {}", new Object[]{Integer.valueOf(READ_AHEAD_SIZE_KB), Double.valueOf(READ_AHEAD_WINDOW), Boolean.valueOf(READ_AHEAD_VECTORED)});
      metrics = new PrefetchingRebufferer.PrefetchingMetrics();
   }

   @VisibleForTesting
   public static class PrefetchingMetrics {
      private final MetricNameFactory factory = new DefaultNameFactory("Prefetching", "");
      final Meter prefetched;
      final Meter skipped;
      final Meter unused;
      final Meter notReady;

      PrefetchingMetrics() {
         this.prefetched = CassandraMetricsRegistry.Metrics.meter(this.factory.createMetricName("Prefetched"));
         this.skipped = CassandraMetricsRegistry.Metrics.meter(this.factory.createMetricName("Skipped"));
         this.unused = CassandraMetricsRegistry.Metrics.meter(this.factory.createMetricName("Unused"));
         this.notReady = CassandraMetricsRegistry.Metrics.meter(this.factory.createMetricName("NotReady"));
      }

      public String toString() {
         return this.prefetched.getCount() == 0L?"No read-ahead yet":String.format("Prefetched: [%s], Skipped: [%s], Unused: [%s] (%.2f), Not ready: [%s] (%.2f)", new Object[]{this.prefetched, this.skipped, this.unused, Double.valueOf((double)this.unused.getCount() / (double)this.prefetched.getCount()), this.notReady, Double.valueOf((double)this.notReady.getCount() / (double)this.prefetched.getCount())});
      }

      @VisibleForTesting
      void reset() {
         this.prefetched.mark(-this.prefetched.getCount());
         this.skipped.mark(-this.skipped.getCount());
         this.unused.mark(-this.unused.getCount());
      }
   }

   private static final class PrefetchedEntry {
      private final long position;
      private final CompletableFuture<Rebufferer.BufferHolder> future;
      private boolean released;

      PrefetchedEntry(long position, CompletableFuture<Rebufferer.BufferHolder> future) {
         this.position = position;
         this.future = future;
         PrefetchingRebufferer.metrics.prefetched.mark();
      }

      public void release() {
         if(!this.released) {
            this.released = true;
            this.future.whenComplete((buffer, error) -> {
               try {
                  if(buffer != null) {
                     buffer.release();
                     PrefetchingRebufferer.metrics.unused.mark();
                  }

                  if(error != null) {
                     PrefetchingRebufferer.logger.debug("Failed to prefetch buffer due to {}", error.getMessage());
                  }
               } catch (Throwable var3) {
                  PrefetchingRebufferer.logger.debug("Failed to release prefetched buffer due to {}", var3.getMessage());
               }

            });
         }
      }

      public String toString() {
         return String.format("Position: %d, Status: %s", new Object[]{Long.valueOf(this.position), Boolean.valueOf(this.future.isDone())});
      }
   }
}
