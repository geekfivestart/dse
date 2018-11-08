package org.apache.cassandra.io.util;

import com.google.common.util.concurrent.RateLimiter;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.db.mos.MemoryLockedBuffer;
import org.apache.cassandra.db.mos.MemoryOnlyStatus;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.RefCounted;
import org.apache.cassandra.utils.concurrent.SharedCloseableImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileHandle extends SharedCloseableImpl {
   private static final Logger logger = LoggerFactory.getLogger(FileHandle.class);
   public final AsynchronousChannelProxy channel;
   public final long onDiskLength;
   private final RebuffererFactory rebuffererFactory;
   private final Optional<CompressionMetadata> compressionMetadata;
   @Nullable
   private MmappedRegions regions;

   private FileHandle(FileHandle.Cleanup cleanup, AsynchronousChannelProxy channel, RebuffererFactory rebuffererFactory, CompressionMetadata compressionMetadata, long onDiskLength, MmappedRegions regions) {
      super((RefCounted.Tidy)cleanup);
      this.rebuffererFactory = rebuffererFactory;
      this.channel = channel;
      this.compressionMetadata = Optional.ofNullable(compressionMetadata);
      this.onDiskLength = onDiskLength;
      this.regions = regions;
   }

   private FileHandle(FileHandle copy) {
      super((SharedCloseableImpl)copy);
      this.channel = copy.channel;
      this.rebuffererFactory = copy.rebuffererFactory;
      this.compressionMetadata = copy.compressionMetadata;
      this.onDiskLength = copy.onDiskLength;
      this.regions = copy.regions;
   }

   public String path() {
      return this.channel.filePath();
   }

   public boolean mmapped() {
      return this.regions != null;
   }

   public long dataLength() {
      Optional var10000 = this.compressionMetadata.map((c) -> {
         return Long.valueOf(c.dataLength);
      });
      RebuffererFactory var10001 = this.rebuffererFactory;
      this.rebuffererFactory.getClass();
      return ((Long)var10000.orElseGet(var10001::fileLength)).longValue();
   }

   public RebuffererFactory rebuffererFactory() {
      return this.rebuffererFactory;
   }

   public Optional<CompressionMetadata> compressionMetadata() {
      return this.compressionMetadata;
   }

   public void addTo(Ref.IdentityCollection identities) {
      super.addTo(identities);
      this.compressionMetadata.ifPresent((metadata) -> {
         metadata.addTo(identities);
      });
   }

   public FileHandle sharedCopy() {
      return new FileHandle(this);
   }

   public RandomAccessReader createReader() {
      return this.createReader(Rebufferer.ReaderConstraint.NONE);
   }

   public RandomAccessReader createReader(Rebufferer.ReaderConstraint constraint) {
      return new RandomAccessReader(this.instantiateRebufferer(FileAccessType.RANDOM, (RateLimiter)null), constraint);
   }

   public RandomAccessReader createReader(Rebufferer.ReaderConstraint constraint, FileAccessType accessType) {
      return new RandomAccessReader(this.instantiateRebufferer(accessType, (RateLimiter)null), constraint);
   }

   public RandomAccessReader createReader(RateLimiter limiter) {
      return this.createReader(limiter, FileAccessType.RANDOM);
   }

   public RandomAccessReader createReader(RateLimiter limiter, FileAccessType accessType) {
      return new RandomAccessReader(this.instantiateRebufferer(accessType, limiter), Rebufferer.ReaderConstraint.NONE);
   }

   public FileDataInput createReader(long position, Rebufferer.ReaderConstraint rc) {
      RandomAccessReader reader = this.createReader(rc);
      reader.seek(position);
      return reader;
   }

   public void dropPageCache(long before) {
      long position = ((Long)this.compressionMetadata.map((metadata) -> {
         return before >= metadata.dataLength?Long.valueOf(0L):Long.valueOf(metadata.chunkFor(before).offset);
      }).orElse(Long.valueOf(before))).longValue();
      this.channel.tryToSkipCache(0L, position);
   }

   private Rebufferer instantiateRebufferer(FileAccessType accessType, RateLimiter limiter) {
      Rebufferer rebufferer = this.rebuffererFactory.instantiateRebufferer(accessType);
      if(limiter != null) {
         rebufferer = new LimitingRebufferer((Rebufferer)rebufferer, limiter, 65536);
      }

      return (Rebufferer)rebufferer;
   }

   public void lock(MemoryOnlyStatus instance) {
      if(this.regions != null) {
         this.regions.lock(instance);
      } else {
         instance.reportFailedAttemptedLocking(this.onDiskLength);
      }

   }

   public void unlock(MemoryOnlyStatus instance) {
      if(this.regions != null) {
         this.regions.unlock(instance);
      } else {
         instance.clearFailedAttemptedLocking(this.onDiskLength);
      }

   }

   public List<MemoryLockedBuffer> getLockedMemory() {
      return this.regions != null?this.regions.getLockedMemory():Collections.singletonList(MemoryLockedBuffer.failed(0L, this.onDiskLength));
   }

   public String toString() {
      return this.getClass().getSimpleName() + "(path='" + this.path() + '\'' + ", length=" + this.rebuffererFactory.fileLength() + ')';
   }

   public static class Builder implements AutoCloseable {
      private final String path;
      private AsynchronousChannelProxy channel;
      private CompressionMetadata compressionMetadata;
      private MmappedRegions regions;
      private ChunkCache chunkCache;
      private int bufferSize = 4096;
      private BufferType bufferType;
      private boolean mmapped;
      private boolean compressed;

      public Builder(String path) {
         this.bufferType = BufferType.OFF_HEAP;
         this.mmapped = false;
         this.compressed = false;
         this.path = path;
      }

      public Builder(AsynchronousChannelProxy channel) {
         this.bufferType = BufferType.OFF_HEAP;
         this.mmapped = false;
         this.compressed = false;
         this.channel = channel;
         this.path = channel.filePath();
      }

      public FileHandle.Builder compressed(boolean compressed) {
         this.compressed = compressed;
         return this;
      }

      public FileHandle.Builder withChunkCache(ChunkCache chunkCache) {
         this.chunkCache = chunkCache;
         return this;
      }

      public FileHandle.Builder withCompressionMetadata(CompressionMetadata metadata) {
         this.compressed = Objects.nonNull(metadata);
         this.compressionMetadata = metadata;
         return this;
      }

      public FileHandle.Builder mmapped(boolean mmapped) {
         this.mmapped = mmapped;
         return this;
      }

      public FileHandle.Builder bufferSize(int bufferSize) {
         this.bufferSize = bufferSize;
         return this;
      }

      public FileHandle.Builder bufferType(BufferType bufferType) {
         this.bufferType = bufferType;
         return this;
      }

      public FileHandle complete() {
         return this.complete(-1L);
      }

      public FileHandle complete(long overrideLength) {
         if(this.channel == null) {
            this.channel = new AsynchronousChannelProxy(this.path);
         }

         AsynchronousChannelProxy channelCopy = this.channel.sharedCopy();

         try {
            if(this.compressed && this.compressionMetadata == null) {
               this.compressionMetadata = CompressionMetadata.create(channelCopy.filePath());
            }

            long length = overrideLength > 0L?overrideLength:(this.compressed?this.compressionMetadata.compressedFileLength:channelCopy.size());
            Object rebuffererFactory;
            if(length == 0L) {
               rebuffererFactory = new EmptyRebufferer(channelCopy);
            } else if(this.mmapped) {
               ChannelProxy blockingChannel = channelCopy.getBlockingChannel();
               Throwable var8 = null;

               try {
                  if(this.compressed) {
                     this.regions = MmappedRegions.map(blockingChannel, this.compressionMetadata);
                     rebuffererFactory = this.maybeCached(new CompressedChunkReader.Mmap(channelCopy, this.compressionMetadata, this.regions));
                  } else {
                     this.updateRegions(blockingChannel, length);
                     rebuffererFactory = new MmapRebufferer(channelCopy, length, this.regions.sharedCopy());
                  }
               } catch (Throwable var18) {
                  var8 = var18;
                  throw var18;
               } finally {
                  if(blockingChannel != null) {
                     if(var8 != null) {
                        try {
                           blockingChannel.close();
                        } catch (Throwable var17) {
                           var8.addSuppressed(var17);
                        }
                     } else {
                        blockingChannel.close();
                     }
                  }

               }
            } else {
               this.regions = null;
               if(this.compressed) {
                  rebuffererFactory = this.maybeCached(new CompressedChunkReader.Standard(channelCopy, this.compressionMetadata));
               } else {
                  int chunkSize = ChunkCache.roundForCaching(this.bufferSize);
                  rebuffererFactory = this.maybeCached(new SimpleChunkReader(channelCopy, length, this.bufferType, chunkSize));
               }
            }

            FileHandle.Cleanup cleanup = new FileHandle.Cleanup(channelCopy, (RebuffererFactory)rebuffererFactory, this.compressionMetadata, this.chunkCache, this.regions == null?null:this.regions.sharedCopy());
            return new FileHandle(cleanup, channelCopy, (RebuffererFactory)rebuffererFactory, this.compressionMetadata, length, cleanup.regions);
         } catch (Throwable var20) {
            channelCopy.close();
            throw var20;
         }
      }

      public Throwable close(Throwable accumulate) {
         if(!this.compressed && this.regions != null) {
            accumulate = this.regions.close(accumulate);
         }

         return this.channel != null?this.channel.close(accumulate):accumulate;
      }

      public void close() {
         Throwables.maybeFail(this.close((Throwable)null));
      }

      private RebuffererFactory maybeCached(ChunkReader reader) {
         return (RebuffererFactory)(this.chunkCache != null?this.chunkCache.maybeWrap(reader):reader);
      }

      private void updateRegions(ChannelProxy channel, long length) {
         if(this.regions != null && !this.regions.isValid(channel)) {
            Throwable err = this.regions.close((Throwable)null);
            if(err != null) {
               FileHandle.logger.error("Failed to close mapped regions", err);
            }

            this.regions = null;
         }

         if(this.regions == null) {
            this.regions = MmappedRegions.map(channel, length, this.bufferSize);
         } else {
            this.regions.extend(length, this.bufferSize);
         }

      }
   }

   private static class Cleanup implements RefCounted.Tidy {
      final AsynchronousChannelProxy channel;
      final RebuffererFactory rebufferer;
      final CompressionMetadata compressionMetadata;
      final Optional<ChunkCache> chunkCache;
      final MmappedRegions regions;

      private Cleanup(AsynchronousChannelProxy channel, RebuffererFactory rebufferer, CompressionMetadata compressionMetadata, ChunkCache chunkCache, MmappedRegions regions) {
         this.channel = channel;
         this.rebufferer = rebufferer;
         this.compressionMetadata = compressionMetadata;
         this.chunkCache = Optional.ofNullable(chunkCache);
         this.regions = regions;
      }

      public String name() {
         return this.channel.filePath();
      }

      public void tidy() {
         Throwables.perform(new Throwables.DiscreteAction[]{() -> {
            if(this.compressionMetadata != null) {
               this.compressionMetadata.close();
            }

         }, () -> {
            this.channel.close();
         }, () -> {
            this.rebufferer.close();
         }, () -> {
            if(this.regions != null) {
               this.regions.close();
            }

         }});
      }
   }
}
