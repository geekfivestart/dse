package org.apache.cassandra.cache;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Weigher;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.AsynchronousChannelProxy;
import org.apache.cassandra.io.util.ChunkReader;
import org.apache.cassandra.io.util.FileAccessType;
import org.apache.cassandra.io.util.PrefetchingRebufferer;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.RebuffererFactory;
import org.apache.cassandra.metrics.CacheMissMetrics;
import org.apache.cassandra.metrics.Timer;
import org.apache.cassandra.utils.UnsafeByteBufferAccess;
import org.apache.cassandra.utils.memory.BufferPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChunkCacheImpl implements AsyncCacheLoader<ChunkCacheImpl.Key, ChunkCacheImpl.Buffer>, RemovalListener<ChunkCacheImpl.Key, ChunkCacheImpl.Buffer>, CacheSize {
   private static final Logger logger = LoggerFactory.getLogger(ChunkCacheImpl.class);
   private final AsyncLoadingCache<ChunkCacheImpl.Key, ChunkCacheImpl.Buffer> cache;
   private final CacheMissMetrics metrics;
   private final long cacheSize;
   private final BufferPool bufferPool;
   private static final AtomicIntegerFieldUpdater<ChunkCacheImpl.Buffer> referencesUpdater = AtomicIntegerFieldUpdater.newUpdater(ChunkCacheImpl.Buffer.class, "references");

   public ChunkCacheImpl(CacheMissMetrics metrics, long cacheSize) {
      this.cacheSize = cacheSize;
      this.metrics = metrics;
      Executor cleanupExecutor = Executors.newSingleThreadExecutor();
      this.cache = Caffeine.newBuilder().maximumWeight(cacheSize).executor((r) -> {
         if(r.getClass().getSimpleName().equalsIgnoreCase("PerformCleanupTask")) {
            cleanupExecutor.execute(r);
         } else {
            r.run();
         }

      }).weigher((key, buffer) -> {
         return ((ChunkCacheImpl.Buffer)buffer).buffer.capacity();
      }).removalListener(this).buildAsync(this);
      this.bufferPool = new BufferPool();
   }

   public CompletableFuture<ChunkCacheImpl.Buffer> asyncLoad(ChunkCacheImpl.Key key, Executor executor) {
      ChunkReader rebufferer = key.file;
      this.metrics.misses.mark();
      Timer.Context ctx = this.metrics.missLatency.timer();

      try {
         ByteBuffer buffer = this.bufferPool.get(key.file.chunkSize());

         assert !buffer.isDirect() || (UnsafeByteBufferAccess.getAddress(buffer) & 511L) == 0L : "Buffer from pool is not properly aligned!";

         return rebufferer.readChunk(key.position, buffer).thenApply((b) -> {
            return new ChunkCacheImpl.Buffer(key, b);
         }).whenComplete((b, t) -> {
            ctx.close();
            if(t != null) {
               this.bufferPool.put(buffer);
            }

         });
      } catch (Throwable var6) {
         ctx.close();
         throw var6;
      }
   }

   public void onRemoval(ChunkCacheImpl.Key key, ChunkCacheImpl.Buffer buffer, RemovalCause cause) {
      buffer.release();
   }

   public void reset() {
      this.cache.synchronous().invalidateAll();
      this.metrics.reset();
   }

   public void close() {
      this.cache.synchronous().invalidateAll();
   }

   public RebuffererFactory wrap(ChunkReader file) {
      return new ChunkCacheImpl.CachingRebufferer(file);
   }

   @VisibleForTesting
   public void invalidateFile(String fileName) {
      this.cache.synchronous().invalidateAll(Iterables.filter(this.cache.synchronous().asMap().keySet(), (x) -> {
         return x.path().equals(fileName);
      }));
   }

   public long capacity() {
      return this.cacheSize;
   }

   public void setCapacity(long capacity) {
      throw new UnsupportedOperationException("Chunk cache size cannot be changed.");
   }

   public int size() {
      return this.cache.synchronous().asMap().size();
   }

   public long weightedSize() {
      Optional var10000 = this.cache.synchronous().policy().eviction().map((policy) -> {
         OptionalLong var10000 = policy.weightedSize();
         LoadingCache var10001 = this.cache.synchronous();
         var10001.getClass();
         return Long.valueOf(var10000.orElseGet(var10001::estimatedSize));
      });
      LoadingCache var10001 = this.cache.synchronous();
      var10001.getClass();
      return ((Long)var10000.orElseGet(var10001::estimatedSize)).longValue();
   }

   class CachingRebufferer implements Rebufferer, RebuffererFactory {
      private final ChunkReader source;
      final long alignmentMask;

      public CachingRebufferer(ChunkReader file) {
         this.source = file;
         int chunkSize = file.chunkSize();

         assert Integer.bitCount(chunkSize) == 1 : String.format("%d must be a power of two", new Object[]{Integer.valueOf(chunkSize)});

         this.alignmentMask = (long)(-chunkSize);
      }

      public ChunkCacheImpl.Buffer rebuffer(long position) {
         try {
            ChunkCacheImpl.this.metrics.requests.mark();
            long pageAlignedPos = position & this.alignmentMask;
            ChunkCacheImpl.Buffer buf = null;
            ChunkCacheImpl.Key pageKey = new ChunkCacheImpl.Key(this.source, pageAlignedPos);
            ChunkCacheImpl.Buffer page = null;
            int spin = 0;

            while(page == null || (buf = page.reference()) == null) {
               page = (ChunkCacheImpl.Buffer)ChunkCacheImpl.this.cache.get(pageKey).join();
               if(page != null && buf == null) {
                  ++spin;
                  if(spin == 1024) {
                     ChunkCacheImpl.logger.error("Spinning for {}", pageKey);
                  }
               }
            }

            return buf;
         } catch (Throwable var9) {
            Throwables.propagateIfInstanceOf(var9.getCause(), CorruptSSTableException.class);
            throw Throwables.propagate(var9);
         }
      }

      public ChunkCacheImpl.Buffer rebuffer(long position, Rebufferer.ReaderConstraint rc) {
         if(rc != Rebufferer.ReaderConstraint.ASYNC) {
            return this.rebuffer(position);
         } else {
            ChunkCacheImpl.this.metrics.requests.mark();
            long pageAlignedPos = position & this.alignmentMask;
            ChunkCacheImpl.Key key = new ChunkCacheImpl.Key(this.source, pageAlignedPos);
            CompletableFuture<ChunkCacheImpl.Buffer> asyncBuffer = ChunkCacheImpl.this.cache.get(key);
            if(asyncBuffer.isDone()) {
               ChunkCacheImpl.Buffer buf = (ChunkCacheImpl.Buffer)asyncBuffer.join();
               if(buf != null && (buf = buf.reference()) != null) {
                  return buf;
               }

               asyncBuffer = ChunkCacheImpl.this.cache.get(key);
            }

            ChunkCacheImpl.this.metrics.notInCacheExceptions.mark();
            throw new Rebufferer.NotInCacheException(this.channel(), asyncBuffer.thenAccept((buffer) -> {
            }), key.path(), key.position);
         }
      }

      public CompletableFuture<Rebufferer.BufferHolder> rebufferAsync(long position) {
         ChunkCacheImpl.this.metrics.requests.mark();
         CompletableFuture<Rebufferer.BufferHolder> ret = new CompletableFuture();
         this.getPage(position, ret, 0);
         return ret;
      }

      private void getPage(long position, CompletableFuture<Rebufferer.BufferHolder> ret, int numAttempts) {
         long pageAlignedPos = position & this.alignmentMask;
         ChunkCacheImpl.this.cache.get(new ChunkCacheImpl.Key(this.source, pageAlignedPos)).whenComplete((page, error) -> {
            if(error != null) {
               ret.completeExceptionally(error);
            } else {
               Rebufferer.BufferHolder buffer = page.reference();
               if(buffer != null) {
                  ret.complete(buffer);
               } else if(numAttempts < 1024) {
                  TPC.bestTPCScheduler().scheduleDirect(() -> {
                     this.getPage(position, ret, numAttempts + 1);
                  });
               } else {
                  ret.completeExceptionally(new IllegalStateException("Failed to acquire buffer from cache after 1024 attempts"));
               }

            }
         });
      }

      public int rebufferSize() {
         return this.source.chunkSize();
      }

      public void invalidate(long position) {
         long pageAlignedPos = position & this.alignmentMask;
         ChunkCacheImpl.this.cache.synchronous().invalidate(new ChunkCacheImpl.Key(this.source, pageAlignedPos));
      }

      public Rebufferer instantiateRebufferer(FileAccessType accessType) {
         if(accessType != FileAccessType.RANDOM && !this.source.isMmap() && PrefetchingRebufferer.READ_AHEAD_SIZE_KB > 0) {
            AsynchronousChannelProxy channel = this.source.channel().maybeBatched(PrefetchingRebufferer.READ_AHEAD_VECTORED);
            return new PrefetchingRebufferer(ChunkCacheImpl.this.new CachingRebufferer(this.source.withChannel(channel)), channel);
         } else {
            return this;
         }
      }

      public void close() {
         this.source.close();
      }

      public void closeReader() {
      }

      public AsynchronousChannelProxy channel() {
         return this.source.channel();
      }

      public long fileLength() {
         return this.source.fileLength();
      }

      public double getCrcCheckChance() {
         return this.source.getCrcCheckChance();
      }

      public String toString() {
         return "CachingRebufferer:" + this.source;
      }
   }

   public class Buffer implements Rebufferer.BufferHolder {
      private final ByteBuffer buffer;
      private final ChunkCacheImpl.Key key;
      volatile int references = 1;

      public Buffer(ChunkCacheImpl.Key key, ByteBuffer buffer) {
         this.key = key;
         this.buffer = buffer;
      }

      ChunkCacheImpl.Buffer reference() {
         int refCount;
         do {
            refCount = this.references;
            if(refCount == 0) {
               return null;
            }
         } while(!ChunkCacheImpl.referencesUpdater.compareAndSet(this, refCount, refCount + 1));

         return this;
      }

      public ByteBuffer buffer() {
         assert this.references > 0;

         return this.buffer.duplicate();
      }

      public long offset() {
         return this.key.position;
      }

      public void release() {
         if(ChunkCacheImpl.referencesUpdater.decrementAndGet(this) == 0) {
            ChunkCacheImpl.this.bufferPool.put(this.buffer);
         }

      }

      public String toString() {
         return "ChunkCache$Buffer(" + this.key + ")";
      }
   }

   static class Key {
      final ChunkReader file;
      final long position;

      public Key(ChunkReader file, long position) {
         this.file = file;
         this.position = position;
      }

      public int hashCode() {
         int prime = true;
         int result = 1;
         int result = 31 * result + this.path().hashCode();
         result = 31 * result + this.file.getClass().hashCode();
         result = 31 * result + Long.hashCode(this.position);
         return result;
      }

      public boolean equals(Object obj) {
         if(this == obj) {
            return true;
         } else if(obj == null) {
            return false;
         } else {
            ChunkCacheImpl.Key other = (ChunkCacheImpl.Key)obj;
            return this.position == other.position && this.file.getClass() == other.file.getClass() && this.path().equals(other.path());
         }
      }

      public String path() {
         return this.file.channel().filePath();
      }

      public String toString() {
         return this.path() + '@' + this.position;
      }
   }
}
