package org.apache.cassandra.io.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.primitives.Ints;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.CompletableFuture;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.compress.CorruptBlockException;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.utils.ChecksumType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CompressedChunkReader extends AbstractReaderFileProxy implements ChunkReader {
   static final int CHECKSUM_BYTES = 4;
   private static final Logger logger = LoggerFactory.getLogger(CompressedChunkReader.class);
   final CompressionMetadata metadata;
   final int maxCompressedLength;

   protected CompressedChunkReader(AsynchronousChannelProxy channel, CompressionMetadata metadata) {
      super(channel, metadata.dataLength);
      this.metadata = metadata;
      this.maxCompressedLength = metadata.maxCompressedLength();

      assert Integer.bitCount(metadata.chunkLength()) == 1;

   }

   @VisibleForTesting
   public double getCrcCheckChance() {
      return this.metadata.parameters.getCrcCheckChance();
   }

   public boolean shouldCheckCrc() {
      return this.metadata.parameters.shouldCheckCrc();
   }

   public String toString() {
      return String.format("CompressedChunkReader.%s(%s - %s, chunk length %d, data length %d)", new Object[]{this.getClass().getSimpleName(), this.channel.filePath(), this.metadata.compressor().getClass().getSimpleName(), Integer.valueOf(this.metadata.chunkLength()), Long.valueOf(this.metadata.dataLength)});
   }

   public int chunkSize() {
      return this.metadata.chunkLength();
   }

   public BufferType preferredBufferType() {
      return TPC.USE_AIO?BufferType.OFF_HEAP_ALIGNED:this.metadata.compressor().preferredBufferType();
   }

   public Rebufferer instantiateRebufferer(FileAccessType accessType) {
      if(accessType != FileAccessType.RANDOM && PrefetchingRebufferer.READ_AHEAD_SIZE_KB > 0) {
         AsynchronousChannelProxy channel = this.channel.maybeBatched(PrefetchingRebufferer.READ_AHEAD_VECTORED);
         return new PrefetchingRebufferer(new BufferManagingRebufferer.Aligned(this.withChannel(channel)), channel);
      } else {
         return new BufferManagingRebufferer.Aligned(this);
      }
   }

   public static class Mmap extends CompressedChunkReader {
      protected final MmappedRegions regions;

      public Mmap(AsynchronousChannelProxy channel, CompressionMetadata metadata, MmappedRegions regions) {
         super(channel, metadata);
         this.regions = regions;
      }

      public CompletableFuture<ByteBuffer> readChunk(long position, ByteBuffer uncompressed) {
         CompletableFuture future = new CompletableFuture();

         try {
            future.complete(this.doReadChunk(position, uncompressed));
         } catch (Throwable var6) {
            if(TPCUtils.isWouldBlockException(var6)) {
               TPC.ioScheduler().execute(() -> {
                  try {
                     future.complete(this.doReadChunk(position, uncompressed));
                  } catch (Throwable var6) {
                     future.completeExceptionally(var6);
                  }

               }, TPCTaskType.READ_DISK_ASYNC);
            } else {
               future.completeExceptionally(var6);
            }
         }

         return future;
      }

      private ByteBuffer doReadChunk(long position, ByteBuffer uncompressed) {
         try {
            assert (position & (long)(-uncompressed.capacity())) == position;

            assert position <= this.fileLength;

            CompressionMetadata.Chunk chunk = this.metadata.chunkFor(position);
            MmappedRegions.Region region = this.regions.floor(chunk.offset);
            long segmentOffset = region.offset();
            int chunkOffset = Ints.checkedCast(chunk.offset - segmentOffset);
            ByteBuffer compressedChunk = region.buffer();
            compressedChunk.position(chunkOffset).limit(chunkOffset + chunk.length);
            uncompressed.clear();

            try {
               if(this.shouldCheckCrc()) {
                  int checksum = (int)ChecksumType.CRC32.of(compressedChunk);
                  compressedChunk.limit(compressedChunk.capacity());
                  if(compressedChunk.getInt() != checksum) {
                     throw new CorruptBlockException(this.channel.filePath(), chunk);
                  }

                  compressedChunk.position(chunkOffset).limit(chunkOffset + chunk.length);
               }

               if(chunk.length < this.maxCompressedLength) {
                  this.metadata.compressor().uncompress(compressedChunk, uncompressed);
               } else {
                  uncompressed.put(compressedChunk);
               }
            } catch (IOException var11) {
               throw new CorruptBlockException(this.channel.filePath(), chunk, var11);
            }

            uncompressed.flip();
            return uncompressed;
         } catch (CorruptBlockException var12) {
            uncompressed.position(0).limit(0);
            throw new CorruptSSTableException(var12, this.channel.filePath());
         }
      }

      public void close() {
         this.regions.closeQuietly();
         super.close();
      }

      public boolean isMmap() {
         return true;
      }

      public ChunkReader withChannel(AsynchronousChannelProxy channel) {
         throw new UnsupportedOperationException("Recreating a reader with a new channel not yet implemented for mmap");
      }
   }

   public static class Standard extends CompressedChunkReader {
      final Supplier<Integer> bufferSize;

      public Standard(AsynchronousChannelProxy channel, CompressionMetadata metadata) {
         super(channel, metadata);
         this.bufferSize = Suppliers.memoize(() -> {
            int size = Math.min(this.maxCompressedLength, metadata.compressor().initialCompressedBufferLength(metadata.chunkLength()));
            size = Math.max(size, metadata.chunkLength());
            size += 4;
            return Integer.valueOf(this.roundUpToBlockSize(size) + this.sectorSize);
         });
      }

      public CompletableFuture<ByteBuffer> readChunk(long position, ByteBuffer uncompressed) {
         CompletableFuture<ByteBuffer> ret = new CompletableFuture();
         ChunkReader.BufferHandle bufferHandle = this.getScratchHandle();

         try {
            this.doReadChunk(position, uncompressed, ret, bufferHandle);
         } catch (Throwable var7) {
            if(TPCUtils.isWouldBlockException(var7)) {
               TPC.ioScheduler().execute(() -> {
                  try {
                     this.doReadChunk(position, uncompressed, ret, bufferHandle);
                  } catch (Throwable var7) {
                     this.error(var7, uncompressed, ret, bufferHandle);
                  }

               }, TPCTaskType.READ_DISK_ASYNC);
            } else {
               this.error(var7, uncompressed, ret, bufferHandle);
            }
         }

         return ret;
      }

      private void doReadChunk(long position, final ByteBuffer uncompressed, final CompletableFuture<ByteBuffer> futureBuffer, final ChunkReader.BufferHandle bufferHandle) {
         assert (position & (long)(-this.metadata.chunkLength())) == position;

         assert position <= this.fileLength;

         final CompressionMetadata.Chunk chunk = this.metadata.chunkFor(position);
         final ByteBuffer compressed = bufferHandle.get(((Integer)this.bufferSize.get()).intValue());
         long alignedOffset = this.roundDownToBlockSize(chunk.offset);
         final int alignmentShift = Ints.checkedCast(chunk.offset - alignedOffset);
         compressed.clear();
         compressed.limit(this.roundUpToBlockSize(chunk.length + alignmentShift + 4));
         this.channel.read(compressed, alignedOffset, new CompletionHandler<Integer, ByteBuffer>() {
            public void completed(Integer result, ByteBuffer attachment) {
               try {
                  if(result.intValue() < chunk.length + alignmentShift + 4) {
                     throw new CorruptBlockException(Standard.this.channel.filePath() + " result = " + result, chunk);
                  }

                  compressed.limit(chunk.length + alignmentShift);
                  compressed.position(alignmentShift);
                  uncompressed.clear();
                  if(Standard.this.shouldCheckCrc()) {
                     int checksum = (int)ChecksumType.CRC32.of(compressed);
                     compressed.limit(compressed.capacity());
                     if(compressed.getInt() != checksum) {
                        throw new CorruptBlockException(Standard.this.channel.filePath(), chunk);
                     }

                     compressed.limit(chunk.length + alignmentShift).position(alignmentShift);
                  }

                  if(chunk.length < Standard.this.maxCompressedLength) {
                     try {
                        Standard.this.metadata.compressor().uncompress(compressed, uncompressed);
                     } catch (IOException var4) {
                        throw new CorruptBlockException(Standard.this.channel.filePath(), chunk, var4);
                     }
                  } else {
                     uncompressed.put(compressed);
                  }

                  uncompressed.flip();
               } catch (Throwable var5) {
                  if(TPCUtils.isWouldBlockException(var5)) {
                     TPC.ioScheduler().execute(() -> {
                        this.completed(result, attachment);
                     }, TPCTaskType.READ_DISK_ASYNC);
                  } else {
                     Standard.this.error(var5, uncompressed, futureBuffer, bufferHandle);
                  }

                  return;
               }

               bufferHandle.recycle();
               if(!futureBuffer.complete(uncompressed)) {
                  CompressedChunkReader.logger.warn("Failed to complete read from {}, already timed out.", Standard.this.channel.filePath);
               }

            }

            public void failed(Throwable t, ByteBuffer attachment) {
               Standard.this.error(t, uncompressed, futureBuffer, bufferHandle);
            }
         });
      }

      void error(Throwable t, ByteBuffer uncompressed, CompletableFuture<ByteBuffer> futureBuffer, ChunkReader.BufferHandle bufferHandle) {
         uncompressed.position(0).limit(0);
         bufferHandle.recycle();
         futureBuffer.completeExceptionally(new CorruptSSTableException(t, this.channel.filePath()));
      }

      public boolean isMmap() {
         return false;
      }

      public ChunkReader withChannel(AsynchronousChannelProxy channel) {
         return new CompressedChunkReader.Standard(channel, this.metadata);
      }
   }
}
